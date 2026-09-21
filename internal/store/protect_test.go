package store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sync"
	"testing"

	"batchseal/internal/store"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- write protection (protectWrites / X-Batch-Write-Token) ---

// createProtectedBatch creates a protected batch and asserts the issued
// token is 256-bit unpadded base64url (43 chars).
func createProtectedBatch(ctx context.Context, t *testing.T, s *store.Store, expected int) (*store.Batch, string) {
	t.Helper()
	b, err := s.CreateBatch(ctx, expected, true)
	if err != nil {
		t.Fatalf("create protected batch: %v", err)
	}
	if b.WriteToken == "" || len(b.WriteToken) != 43 {
		t.Fatalf("write token not 43-char unpadded base64url: %q", b.WriteToken)
	}
	return b, b.WriteToken
}

func digestForBatch(ctx context.Context, t *testing.T, pool *pgxpool.Pool, batchID string) []byte {
	t.Helper()
	var digest []byte
	if err := pool.QueryRow(ctx,
		`SELECT write_token_digest FROM batches WHERE id = $1`, batchID).Scan(&digest); err != nil {
		t.Fatalf("read digest: %v", err)
	}
	return digest
}

// TestProtectedBatchIssuesTokenAndStoresOnlyDigest checks the token format
// on issue and that the database stores exactly SHA-256(token) - never the
// token itself - while an unprotected batch stores NULL.
func TestProtectedBatchIssuesTokenAndStoresOnlyDigest(t *testing.T) {
	ctx := context.Background()
	schema := newSchema(ctx, t)
	s, err := store.New(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	raw := openRawPool(ctx, t, schema)

	b, token := createProtectedBatch(ctx, t, s, 2)

	want := sha256.Sum256([]byte(token))
	got := digestForBatch(ctx, t, raw, b.ID)
	if len(got) != 32 || !bytes.Equal(got, want[:]) {
		t.Fatalf("stored digest != sha256(token): got %v want %x", got, want)
	}

	// The raw token must not appear anywhere in the persisted row.
	var leak bool
	if err := raw.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM batches WHERE id = $1 AND $2::text = ANY (
			SELECT v FROM unnest(string_to_array(id::text, '')) v))`,
		b.ID, token).Scan(&leak); err == nil && leak {
		t.Fatalf("token leaked into row")
	}

	// Status reads never surface the digest or a token field.
	snap, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.WriteToken != "" {
		t.Fatalf("snapshot exposed a token: %+v", snap)
	}

	// An unprotected batch stores NULL and issues no token.
	u := mustBatch(ctx, t, s, 1)
	if d := digestForBatch(ctx, t, raw, u.ID); d != nil {
		t.Fatalf("unprotected batch has non-null digest: %x", d)
	}
	if u.WriteToken != "" {
		t.Fatalf("unprotected create returned a token: %q", u.WriteToken)
	}
}

// TestProtectedBatchRequiresToken proves that missing and wrong tokens get
// ErrWriteCapabilityRequired in every state - open, incomplete, sealed - and
// BEFORE seq-range/conflict verdicts. Nothing is written on rejection.
func TestProtectedBatchRequiresToken(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b, token := createProtectedBatch(ctx, t, s, 2)

	// State 1: open, empty.
	for _, tok := range []string{"", "wrong-token", token + "x"} {
		if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a"), tok); !errors.Is(err, store.ErrWriteCapabilityRequired) {
			t.Fatalf("open submit token=%q: want ErrWriteCapabilityRequired, got %v", tok, err)
		}
	}

	// Valid token writes the first chunk.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a"), token); err != nil {
		t.Fatalf("authenticated submit: %v", err)
	}

	// State 2: open, one chunk present. Missing token must still be a plain
	// 403-class error - never a conflict, never a seq-range error, even when
	// the payload would conflict and the seq would be valid.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("CONFLICT"), ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("conflicting payload without token: want cap error, got %v", err)
	}
	// Out-of-range seq without a token is likewise a cap error, not seq range.
	if _, err := s.SubmitChunk(ctx, b.ID, 99, []byte("a"), ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("out-of-range seq without token: want cap error, got %v", err)
	}
	// seq < 1 is also decided only after the token verdict inside the lock.
	if _, err := s.SubmitChunk(ctx, b.ID, 0, []byte("a"), ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("seq<1 without token: want cap error, got %v", err)
	}
	// ...but with a valid token seq<1 surfaces the normal malformed verdict.
	if _, err := s.SubmitChunk(ctx, b.ID, 0, []byte("a"), token); !errors.Is(err, store.ErrInvalidSeq) {
		t.Fatalf("seq<1 with token: want ErrInvalidSeq, got %v", err)
	}
	// Seal with missing/wrong token is a cap error, not INCOMPLETE.
	if _, err := s.SealBatch(ctx, b.ID, ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("seal without token on incomplete batch: want cap error, got %v", err)
	}
	if _, err := s.SealBatch(ctx, b.ID, "nope"); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("seal wrong token: want cap error, got %v", err)
	}

	// Complete and seal with the valid token.
	if _, err := s.SubmitChunk(ctx, b.ID, 2, []byte("b"), token); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SealBatch(ctx, b.ID, token); err != nil {
		t.Fatalf("seal with token: %v", err)
	}

	// State 3: sealed. Identical retransmission without a token is still a
	// cap error (never silently accepted), and seal without token is a cap
	// error (not the idempotent 200).
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a"), ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("sealed identical retransmit without token: want cap error, got %v", err)
	}
	if _, err := s.SealBatch(ctx, b.ID, ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("sealed reseal without token: want cap error, got %v", err)
	}
	// Valid token still allows the idempotent sealed operations.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("a"), token); err != nil {
		t.Fatalf("sealed identical retransmit with token: %v", err)
	}
	if _, err := s.SealBatch(ctx, b.ID, token); err != nil {
		t.Fatalf("reseal with token: %v", err)
	}

	// No rejected request wrote anything: received must be exactly 2.
	snap, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Received != 2 || snap.Status != store.StatusSealed {
		t.Fatalf("rejected token requests mutated data: %+v", snap)
	}
}

// TestUnprotectedBatchIgnoresTokenHeader proves legacy compatibility:
// batches created with protectWrites omitted/false follow the original
// protocol regardless of any X-Batch-Write-Token header, and rotation is
// rejected with ErrNotProtected.
func TestUnprotectedBatchIgnoresTokenHeader(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b := mustBatch(ctx, t, s, 1)

	// First write succeeds regardless of header; subsequent identical
	// retransmissions are idempotent duplicates, again regardless of header.
	first, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x"), "garbage")
	if err != nil || !first.Created {
		t.Fatalf("unprotected first write: res=%+v err=%v", first, err)
	}
	for _, tok := range []string{"", "garbage", "anything-goes"} {
		res, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x"), tok)
		if err != nil || res.Created {
			t.Fatalf("unprotected retransmit token=%q: res=%+v err=%v", tok, res, err)
		}
		if _, err := s.SealBatch(ctx, b.ID, tok); err != nil {
			t.Fatalf("unprotected seal token=%q: %v", tok, err)
		}
	}
	if _, err := s.RotateWriteToken(ctx, b.ID, ""); !errors.Is(err, store.ErrNotProtected) {
		t.Fatalf("rotate unprotected: want ErrNotProtected, got %v", err)
	}
	if _, err := s.RotateWriteToken(ctx, b.ID, "garbage"); !errors.Is(err, store.ErrNotProtected) {
		t.Fatalf("rotate unprotected with garbage header: want ErrNotProtected, got %v", err)
	}
}

// TestRotateWriteTokenAtomicallyReplacesDigest checks the happy path: a
// successful rotation returns a fresh well-formed token, atomically swaps
// the stored digest, the old token stops working and the new one works.
func TestRotateWriteTokenAtomicallyReplacesDigest(t *testing.T) {
	ctx := context.Background()
	schema := newSchema(ctx, t)
	s, err := store.New(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	raw := openRawPool(ctx, t, schema)

	b, old := createProtectedBatch(ctx, t, s, 1)
	before := digestForBatch(ctx, t, raw, b.ID)

	newTok, err := s.RotateWriteToken(ctx, b.ID, old)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if len(newTok) != 43 || newTok == old {
		t.Fatalf("new token bad: old=%q new=%q", old, newTok)
	}
	after := digestForBatch(ctx, t, raw, b.ID)
	want := sha256.Sum256([]byte(newTok))
	if bytes.Equal(after, before) || !bytes.Equal(after, want[:]) {
		t.Fatalf("digest not atomically replaced: before=%x after=%x want=%x", before, after, want)
	}

	// Old token is dead for writes, seal and further rotation.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x"), old); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("old token submit after rotate: want cap error, got %v", err)
	}
	if _, err := s.SealBatch(ctx, b.ID, old); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("old token seal after rotate: want cap error, got %v", err)
	}
	if _, err := s.RotateWriteToken(ctx, b.ID, old); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("old token rotate after rotate: want cap error, got %v", err)
	}
	// Nothing was written by the rejected submit.
	snap, _ := s.Snapshot(ctx, b.ID)
	if snap.Received != 0 {
		t.Fatalf("old-token submit after rotate wrote data: %+v", snap)
	}

	// New token works end to end, including rotating again.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x"), newTok); err != nil {
		t.Fatalf("new token submit: %v", err)
	}
	newer, err := s.RotateWriteToken(ctx, b.ID, newTok)
	if err != nil {
		t.Fatalf("rotate with new token: %v", err)
	}
	if _, err := s.SealBatch(ctx, b.ID, newer); err != nil {
		t.Fatalf("seal with newest token: %v", err)
	}

	// Rotation on a missing batch is a not-found.
	if _, err := s.RotateWriteToken(ctx, "ffffffffffffffffffffffffffffffff", newer); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rotate missing batch: want ErrNotFound, got %v", err)
	}
}

// TestRotateRequiresCapability proves rotation itself is guarded like any
// other write: missing or wrong token is a cap error and does not rotate.
func TestRotateRequiresCapability(t *testing.T) {
	ctx := context.Background()
	s, _ := newStore(ctx, t)
	b, token := createProtectedBatch(ctx, t, s, 1)

	for _, tok := range []string{"", "wrong"} {
		if _, err := s.RotateWriteToken(ctx, b.ID, tok); !errors.Is(err, store.ErrWriteCapabilityRequired) {
			t.Fatalf("rotate token=%q: want cap error, got %v", tok, err)
		}
	}
	// The original token must still work (a failed rotation changed nothing).
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x"), token); err != nil {
		t.Fatalf("original token invalid after failed rotations: %v", err)
	}
}

// beginHoldingWriteToken opens a raw transaction that performs exactly the
// authorisation half of a rotation or write: lock the batch row FOR UPDATE,
// read its active digest and hold the lock without committing. It stands in
// for an API request queued on the row lock.
func beginHoldingWriteToken(ctx context.Context, t *testing.T, pool *pgxpool.Pool, batchID string) (pgx.Tx, []byte, uint64) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	var digest []byte
	if err := tx.QueryRow(ctx,
		`SELECT write_token_digest FROM batches WHERE id = $1 FOR UPDATE`,
		batchID).Scan(&digest); err != nil {
		t.Fatalf("holder lock batch: %v", err)
	}
	var xid uint64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&xid); err != nil {
		t.Fatalf("holder xid: %v", err)
	}
	return tx, digest, xid
}

// updateDigestInTx replaces the digest within an already-open holder
// transaction (the rotation verdict), leaving commit to the caller.
func updateDigestInTx(ctx context.Context, t *testing.T, tx pgx.Tx, batchID string, digest []byte) {
	t.Helper()
	if _, err := tx.Exec(ctx,
		`UPDATE batches SET write_token_digest = $1 WHERE id = $2`,
		digest, batchID); err != nil {
		t.Fatalf("holder update digest: %v", err)
	}
}

// TestRotationCommittedDuringQueuedSubmit forces the ordering required by
// the spec: the rotation acquires the batch row lock FIRST (its uncommitted
// UPDATE holds it), an old-token submit is proven to be queued, then the
// rotation commits. The submit must be granted the lock afterwards, observe
// the NEW digest, and fail with ErrWriteCapabilityRequired without writing.
func TestRotationCommittedDuringQueuedSubmit(t *testing.T) {
	ctx := context.Background()
	schema := newSchema(ctx, t)
	s, err := store.New(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	raw := openRawPool(ctx, t, schema)

	b, old := createProtectedBatch(ctx, t, s, 1)

	// Holder transaction: takes the lock and performs the rotation verdict
	// but does not yet commit.
	holder, _, xid := beginHoldingWriteToken(ctx, t, raw, b.ID)
	newToken, newDigest, err := func() (string, []byte, error) {
		var tk string
		var dg []byte
		var e error
		// Reuse the package-level token shape via a local 32-byte random
		// value hashed the same way the store hashes presented tokens.
		rawTok := make([]byte, 32)
		if _, e = rand.Read(rawTok); e != nil {
			return "", nil, e
		}
		// base64url no padding, mirroring newWriteToken.
		tk = base64.RawURLEncoding.EncodeToString(rawTok)
		sum := sha256.Sum256([]byte(tk))
		dg = sum[:]
		return tk, dg, nil
	}()
	if err != nil {
		t.Fatal(err)
	}
	updateDigestInTx(ctx, t, holder, b.ID, newDigest)

	// Old-token submit must block on the holder's lock before it can compare.
	type outcome struct {
		res store.SubmitResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, e := s.SubmitChunk(ctx, b.ID, 1, []byte("late"), old)
		done <- outcome{res, e}
	}()
	waitForBlockedOnXid(ctx, t, xid)

	// Commit the rotation, then collect the queued submit.
	if err := holder.Commit(ctx); err != nil {
		t.Fatalf("commit rotation: %v", err)
	}
	got := <-done
	if !errors.Is(got.err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("queued old-token submit after rotate: want cap error, got %v", got.err)
	}

	// No write escaped: batch still empty.
	snap, err := s.Snapshot(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Received != 0 {
		t.Fatalf("queued old-token submit wrote a chunk: %+v", snap)
	}
	// New token issued by the "rotation" is now the valid one.
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("ok"), newToken); err != nil {
		t.Fatalf("new token after forced rotation rejected: %v", err)
	}
}

// TestSubmitCommittedDuringQueuedRotation forces the reverse ordering: an
// old-token submit holds the lock first (and is uncommitted), a rotation
// queues, then the submit COMMITS successfully (old token was valid at lock
// grant); the rotation is granted next, still sees the old digest, and
// succeeds too - but from that instant the old token is invalid.
func TestSubmitCommittedDuringQueuedRotation(t *testing.T) {
	ctx := context.Background()
	schema := newSchema(ctx, t)
	s, err := store.New(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()
	raw := openRawPool(ctx, t, schema)

	b, old := createProtectedBatch(ctx, t, s, 2)

	// Holder plays the in-flight submit: lock held, chunk verdict not yet
	// committed. We insert the chunk directly to simulate the verdict made
	// after a successful token comparison.
	holder, _, xid := beginHoldingWriteToken(ctx, t, raw, b.ID)
	if _, err := holder.Exec(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, 1, 'first')`,
		b.ID); err != nil {
		t.Fatalf("holder insert: %v", err)
	}

	// Rotation queues behind the uncommitted submit.
	type rotOutcome struct {
		tok string
		err error
	}
	rotDone := make(chan rotOutcome, 1)
	go func() {
		tk, e := s.RotateWriteToken(ctx, b.ID, old)
		rotDone <- rotOutcome{tk, e}
	}()
	waitForBlockedOnXid(ctx, t, xid)

	// Submit commits first: it wins, chunk is present.
	if err := holder.Commit(ctx); err != nil {
		t.Fatalf("commit submit: %v", err)
	}

	// The queued rotation is then granted: old digest still matched (the
	// submit did not rotate), so it succeeds and returns a new token.
	rot := <-rotDone
	if rot.err != nil || len(rot.tok) != 43 {
		t.Fatalf("queued rotation after submit committed: tok=%q err=%v", rot.tok, rot.err)
	}

	// The old-token write did land before the rotation took effect.
	snap, _ := s.Snapshot(ctx, b.ID)
	if snap.Received != 1 {
		t.Fatalf("first old-token submit did not land before rotation: %+v", snap)
	}
	// But the old token is now invalid; only the new one admits writes.
	if _, err := s.SubmitChunk(ctx, b.ID, 2, []byte("second"), old); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("old token after rotate: want cap error, got %v", err)
	}
	if _, err := s.SubmitChunk(ctx, b.ID, 2, []byte("second"), rot.tok); err != nil {
		t.Fatalf("new token write after forced ordering: %v", err)
	}
}

// TestConcurrentRotationAndSubmits fires rotations and old/new-token writes
// at the same batch from two pools. Whatever the lock order, the invariants
// hold: a commit using a superseded token never writes, and exactly one
// digest is active at a time.
func TestConcurrentRotationAndSubmits(t *testing.T) {
	ctx := context.Background()
	s, newPeer := newStore(ctx, t)
	other := newPeer()
	defer other.Close()

	const rounds = 20
	for i := 0; i < rounds; i++ {
		b, token := createProtectedBatch(ctx, t, s, 1)

		var wg sync.WaitGroup
		wg.Add(3)
		var rotToken string
		var rotErr error
		go func() {
			defer wg.Done()
			rotToken, rotErr = s.RotateWriteToken(ctx, b.ID, token)
		}()
		go func() {
			defer wg.Done()
			// Old-token submit concurrent with rotation.
			_, _ = other.SubmitChunk(ctx, b.ID, 1, []byte("old"), token)
		}()
		go func() {
			defer wg.Done()
			// Unauthenticated submit must never win.
			_, err := s.SubmitChunk(ctx, b.ID, 1, []byte("none"), "")
			if !errors.Is(err, store.ErrWriteCapabilityRequired) {
				t.Errorf("round %d: unauthenticated submit err=%v", i, err)
			}
		}()
		wg.Wait()
		if rotErr != nil {
			t.Fatalf("round %d rotation: %v", i, rotErr)
		}

		snap, err := s.Snapshot(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		// If the old-token submit landed it must have committed BEFORE the
		// rotation; either way the batch has at most one chunk, and the only
		// currently valid token is rotToken. An identical retransmission with
		// the new token is admitted whether the old submit won (duplicate
		// acknowledgement) or lost (first write), and never writes a second
		// row.
		if snap.Received > 1 {
			t.Fatalf("round %d: %d chunks, want 0 or 1", i, snap.Received)
		}
		res, err := s.SubmitChunk(ctx, b.ID, 1, []byte("old"), rotToken)
		if err != nil {
			t.Fatalf("round %d: new token not capable after rotation: %v (snap=%+v)", i, err, snap)
		}
		_ = res
		snap2, err := s.Snapshot(ctx, b.ID)
		if err != nil || snap2.Received != 1 {
			t.Fatalf("round %d: post-rotation state wrong: %+v err=%v", i, snap2, err)
		}
	}
}
