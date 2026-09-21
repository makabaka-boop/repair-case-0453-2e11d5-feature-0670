// Package store persists batches and chunks in PostgreSQL.
//
// All coordination between API instances happens through database row locks,
// so any number of processes sharing one database reach the same verdict for
// a batch: duplicate chunks are idempotent, conflicting payloads are rejected
// and a batch can only become SEALED when its full chunk set is present.
package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

const (
	StatusOpen   = "OPEN"
	StatusSealed = "SEALED"
)

var (
	ErrNotFound   = errors.New("batch not found")
	ErrConflict   = errors.New("chunk conflict")
	ErrSealed     = errors.New("batch sealed")
	ErrIncomplete = errors.New("batch incomplete")
	ErrSeqRange   = errors.New("sequence out of range")
	// ErrInvalidSeq is the protocol-level seq < 1 verdict. Like every other
	// per-request verdict it is decided inside the locked transaction after
	// the token check, so a write-protected batch answers a missing token
	// with 403 even for a malformed seq. The API maps it to INVALID_SEQ.
	ErrInvalidSeq = errors.New("invalid sequence")
	// ErrWriteCapabilityRequired is returned, after the batch row lock is
	// held, for a write-protected batch when the presented write token is
	// missing or wrong. It deliberately carries no information about which
	// part failed and is always decided before seq range, conflict, gap or
	// sealed verdicts, so none of those states can be probed without the
	// token.
	ErrWriteCapabilityRequired = errors.New("write capability required")
	// ErrNotProtected is returned by rotate on a batch whose
	// write_token_digest is NULL (created before protection or with
	// protectWrites false).
	ErrNotProtected = errors.New("batch not protected")
)

// writeTokenBytes is the entropy size of a write token: 256 bits.
const writeTokenBytes = 32

// tokenEncoding is unpadded base64url, the on-wire representation of a write
// token.
var tokenEncoding = base64.RawURLEncoding

// newWriteToken returns a fresh write token: 256 random bits rendered as
// unpadded base64url (43 characters), together with its SHA-256 digest,
// which is the only thing persisted.
func newWriteToken() (token string, digest []byte, err error) {
	raw := make([]byte, writeTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, err
	}
	token = tokenEncoding.EncodeToString(raw)
	return token, tokenDigest(token), nil
}

// tokenDigest is the stored representation of a token: SHA-256 over the
// exact bytes the client presents in X-Batch-Write-Token.
func tokenDigest(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// checkWriteToken verifies a presented token against the stored digest while
// the caller holds the batch row lock. A NULL digest means the batch is not
// write-protected and every request is admitted. The comparison is constant
// time and is performed against the active digest only: a rotation committed
// by a lock predecessor is what the SELECT ... FOR UPDATE observed once the
// lock was granted under READ COMMITTED.
func checkWriteToken(storedDigest []byte, presented string) bool {
	if len(storedDigest) == 0 {
		return true // old rows and protectWrites:false rows are unprotected
	}
	presentedDigest := tokenDigest(presented)
	return subtle.ConstantTimeCompare(storedDigest, presentedDigest) == 1
}

// Batch is the persisted batch header.
type Batch struct {
	ID             string
	ExpectedChunks int
	Status         string
	CreatedAt      time.Time
	SealedAt       *time.Time
	// WriteToken is set only in the single response that issues a token:
	// create when protectWrites is true, and a successful rotation. It is
	// never populated by a read.
	WriteToken string
}

// batchRow is the locked header used to authorise and decide a write inside
// a transaction.
type batchRow struct {
	expected  int
	status    string
	tokenHash []byte
	sealedAt  *time.Time
	createdAt time.Time
}

// lockedBatch takes the batch row lock and returns the live header. All
// write verdicts (chunk submit, seal, rotate) call this first so token
// comparison and the verdict itself happen under the same lock.
func lockedBatch(ctx context.Context, tx pgx.Tx, batchID string) (batchRow, error) {
	var b batchRow
	err := tx.QueryRow(ctx,
		`SELECT expected_chunks, status, write_token_digest, sealed_at, created_at
		 FROM batches WHERE id = $1 FOR UPDATE`,
		batchID,
	).Scan(&b.expected, &b.status, &b.tokenHash, &b.sealedAt, &b.createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return batchRow{}, ErrNotFound
	}
	if err != nil {
		return batchRow{}, err
	}
	return b, nil
}

// Snapshot is a consistent view of a batch: counts and gaps are computed by
// the same SQL statement.
type Snapshot struct {
	Batch
	Received int
	Gaps     []int
}

// SubmitResult describes one accepted chunk write. A duplicate (same batch,
// seq and byte-identical payload) reports Created == false together with the
// stored confirmation of the original write.
type SubmitResult struct {
	Created    bool
	Seq        int
	Size       int
	ReceivedAt time.Time
}

// Store wraps a connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects, verifies the connection and applies the schema.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	// Correctness depends on READ COMMITTED: every statement takes a fresh
	// snapshot, so once SELECT ... FOR UPDATE is granted on the batch row,
	// later statements in the same transaction observe whatever the lock
	// predecessor committed while this transaction waited. Under REPEATABLE
	// READ the snapshot would be fixed at the first statement - before the
	// lock is granted - so chunks committed by the predecessor would stay
	// invisible: a retransmission would be misjudged as absent and reinserted
	// (unique violation surfacing as 500), and a seal would report gaps for
	// a fully written batch. Writers still serialise on the batch row lock,
	// so check-then-insert cannot race.
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// migrationLockID serialises schema migrations across API instances booting
// at the same time. Concurrent CREATE TABLE IF NOT EXISTS on a fresh database
// otherwise races in the system catalog (SQLSTATE 23505).
const migrationLockID int64 = 0x5B4A_4545_414C_0001

func (s *Store) migrate(ctx context.Context) error {
	// DDL is transactional in PostgreSQL; the advisory lock makes concurrent
	// first boots take turns: the loser blocks here, then finds every object
	// present and the IF NOT EXISTS statements become no-ops.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

// NewBatchID returns a random 128-bit hex identifier.
func NewBatchID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// CreateBatch inserts an OPEN batch. A random ID collision is retried.
//
// When protect is true a fresh write token is issued: its 256-bit entropy is
// returned exactly once (Batch.WriteToken) while only the SHA-256 digest is
// persisted in write_token_digest. When protect is false the column stays
// NULL and the batch follows the unauthenticated legacy protocol.
func (s *Store) CreateBatch(ctx context.Context, expectedChunks int, protect bool) (*Batch, error) {
	const maxAttempts = 3
	var token string
	var digest []byte
	if protect {
		var err error
		token, digest, err = newWriteToken()
		if err != nil {
			return nil, err
		}
	}
	for range maxAttempts {
		id := NewBatchID()
		var b Batch
		err := s.pool.QueryRow(ctx,
			`INSERT INTO batches (id, expected_chunks, write_token_digest)
			 VALUES ($1, $2, $3)
			 RETURNING id, expected_chunks, status, created_at, sealed_at`,
			id, expectedChunks, digest,
		).Scan(&b.ID, &b.ExpectedChunks, &b.Status, &b.CreatedAt, &b.SealedAt)
		if err == nil {
			b.WriteToken = token
			return &b, nil
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			continue
		}
		return nil, err
	}
	return nil, errors.New("could not allocate unique batch id")
}

// SubmitChunk stores a chunk. The batch row is locked for the duration of the
// transaction, serialising it against a concurrent seal or token rotation.
// presentedToken is the raw X-Batch-Write-Token header value. Return values:
//
//   - first write: result.Created == true, nil
//   - retransmission with identical UTF-8 bytes: result.Created == false, nil
//   - same seq, different payload: zero result, ErrConflict
//   - sealed batch, any non-identical write: zero result, ErrSealed
//   - seq < 1: zero result, ErrInvalidSeq
//   - seq > expected: zero result, ErrSeqRange
//   - protected batch with missing/wrong token: zero result,
//     ErrWriteCapabilityRequired
//
// The token verdict is made while holding the row lock and before seq range,
// conflict or sealed-state checks, so an unauthenticated caller cannot probe
// any of those conditions.
func (s *Store) SubmitChunk(ctx context.Context, batchID string, seq int, payload []byte, presentedToken string) (SubmitResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SubmitResult{}, err
	}
	defer tx.Rollback(ctx)

	b, err := lockedBatch(ctx, tx, batchID)
	if err != nil {
		return SubmitResult{}, err
	}
	if !checkWriteToken(b.tokenHash, presentedToken) {
		return SubmitResult{}, ErrWriteCapabilityRequired
	}
	if seq < 1 {
		return SubmitResult{}, ErrInvalidSeq
	}
	if seq > b.expected {
		return SubmitResult{}, ErrSeqRange
	}
	if b.status == StatusSealed {
		// Identical retransmissions of an existing chunk stay allowed;
		// anything else is rejected.
		var existing []byte
		var receivedAt time.Time
		err := tx.QueryRow(ctx,
			`SELECT payload, received_at FROM chunks WHERE batch_id = $1 AND seq = $2`,
			batchID, seq,
		).Scan(&existing, &receivedAt)
		if err == nil && bytes.Equal(existing, payload) {
			return SubmitResult{Created: false, Seq: seq, Size: len(payload), ReceivedAt: receivedAt}, nil
		}
		return SubmitResult{}, ErrSealed
	}

	var existing []byte
	var receivedAt time.Time
	err = tx.QueryRow(ctx,
		`SELECT payload, received_at FROM chunks WHERE batch_id = $1 AND seq = $2`,
		batchID, seq,
	).Scan(&existing, &receivedAt)
	switch {
	case err == nil:
		if !bytes.Equal(existing, payload) {
			return SubmitResult{}, ErrConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return SubmitResult{}, err
		}
		return SubmitResult{Created: false, Seq: seq, Size: len(payload), ReceivedAt: receivedAt}, nil
	case errors.Is(err, pgx.ErrNoRows):
		// proceed to insert
	default:
		return SubmitResult{}, err
	}

	if err := tx.QueryRow(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, $2, $3)
		 RETURNING received_at`,
		batchID, seq, payload,
	).Scan(&receivedAt); err != nil {
		return SubmitResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Created: true, Seq: seq, Size: len(payload), ReceivedAt: receivedAt}, nil
}

// Snapshot returns a consistent status view of a batch. It is a read-only
// operation: it never takes the write lock and never exposes the token
// digest (or token). Write protection therefore does not gate status
// queries.
func (s *Store) Snapshot(ctx context.Context, batchID string) (*Snapshot, error) {
	var snap Snapshot
	err := s.pool.QueryRow(ctx,
		`SELECT id, expected_chunks, status, created_at, sealed_at
		 FROM batches WHERE id = $1`, batchID,
	).Scan(&snap.ID, &snap.ExpectedChunks, &snap.Status, &snap.CreatedAt, &snap.SealedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	snap.Gaps, err = listGaps(ctx, s.pool, batchID, snap.ExpectedChunks)
	if err != nil {
		return nil, err
	}
	snap.Received = snap.ExpectedChunks - len(snap.Gaps)
	return &snap, nil
}

// querier is satisfied by both a pool and a transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// listGaps returns the ascending list of missing sequences: the complement
// of the stored set within [1, expected]. generate_series materialises the
// full range; expected is capped at 10000 so this stays cheap.
func listGaps(ctx context.Context, q querier, batchID string, expected int) ([]int, error) {
	rows, err := q.Query(ctx, `
		SELECT s.seq
		FROM generate_series(1, $1::int) AS s(seq)
		WHERE NOT EXISTS (
			SELECT 1 FROM chunks c WHERE c.batch_id = $2 AND c.seq = s.seq
		)
		ORDER BY s.seq ASC`, expected, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var gaps []int
	for rows.Next() {
		var g int
		if err := rows.Scan(&g); err != nil {
			return nil, err
		}
		gaps = append(gaps, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return gaps, nil
}

// SealBatch atomically seals a batch when its chunk set is complete.
//
// The batch row is locked before the token check and before counting, so a
// seal racing with the final chunk or a token rotation cannot commit a
// SEALED batch that is still missing a piece, and the token verdict always
// uses the digest active at lock grant: an old token rotated while this
// request queued is rejected without sealing. ErrIncomplete is returned
// together with the post-lock snapshot so the API can report the ascending
// gap list. A repeated seal is idempotent and returns the existing SEALED
// snapshot with nil. A missing/wrong token on a protected batch returns
// ErrWriteCapabilityRequired before gaps are inspected.
func (s *Store) SealBatch(ctx context.Context, batchID, presentedToken string) (*Snapshot, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	b, err := lockedBatch(ctx, tx, batchID)
	if err != nil {
		return nil, err
	}
	if !checkWriteToken(b.tokenHash, presentedToken) {
		return nil, ErrWriteCapabilityRequired
	}

	snap := b.snapshot(batchID)
	gaps, err := listGaps(ctx, tx, batchID, snap.ExpectedChunks)
	if err != nil {
		return nil, err
	}
	snap.Gaps = gaps
	snap.Received = snap.ExpectedChunks - len(gaps)

	if snap.Status == StatusSealed {
		// Idempotent: repeated seal returns the existing result unchanged.
		return snap, nil
	}
	if len(snap.Gaps) > 0 {
		return snap, ErrIncomplete
	}
	var sealedAt time.Time
	if err := tx.QueryRow(ctx,
		`UPDATE batches SET status = $1, sealed_at = now() WHERE id = $2
		 RETURNING sealed_at`,
		StatusSealed, batchID,
	).Scan(&sealedAt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	snap.Status = StatusSealed
	snap.SealedAt = &sealedAt
	return snap, nil
}

// RotateWriteToken replaces the active write token of a protected batch.
//
// As with every write, the batch row lock is taken first and the presented
// token is compared in constant time against the active digest. A new
// 256-bit token is generated, its digest atomically replaces the stored one
// inside this same lock, and the new raw token is returned exactly once.
// Until this transaction commits, any queued write holds onto the old
// digest at lock grant and the old token remains valid; once committed, a
// queued old-token writer observes the new digest and fails with
// ErrWriteCapabilityRequired. Rotating an unprotected batch returns
// ErrNotProtected.
func (s *Store) RotateWriteToken(ctx context.Context, batchID, presentedToken string) (newToken string, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	b, err := lockedBatch(ctx, tx, batchID)
	if err != nil {
		return "", err
	}
	if len(b.tokenHash) == 0 {
		return "", ErrNotProtected
	}
	if !checkWriteToken(b.tokenHash, presentedToken) {
		return "", ErrWriteCapabilityRequired
	}

	token, digest, err := newWriteToken()
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE batches SET write_token_digest = $1 WHERE id = $2`,
		digest, batchID); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return token, nil
}

// snapshot builds the Snapshot skeleton (everything but counts and gaps)
// from a locked header.
func (b batchRow) snapshot(id string) *Snapshot {
	return &Snapshot{
		Batch: Batch{
			ID:             id,
			ExpectedChunks: b.expected,
			Status:         b.status,
			CreatedAt:      b.createdAt,
			SealedAt:       b.sealedAt,
		},
	}
}
