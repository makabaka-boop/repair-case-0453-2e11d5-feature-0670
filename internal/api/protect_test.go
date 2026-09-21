package api_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"batchseal/internal/api"
	"batchseal/internal/store"
	"batchseal/internal/testsupport"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// hreq issues one JSON HTTP request with an optional X-Batch-Write-Token
// header (empty means the header is absent).
func hreq(t *testing.T, method, url string, body any, token string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Batch-Write-Token", token)
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("decode %q: %v", data, err)
		}
	}
	return resp.StatusCode, parsed
}

// TestHTTPProtectWritesProtocol covers the full protected-batch protocol on
// the wire: one-time token on 201, no token on status reads, 403 before
// every state verdict, authenticated writes/seal, and identical 403 bodies
// in the open, incomplete and sealed states.
func TestHTTPProtectWritesProtocol(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()

	// protectWrites:true issues a token; false and omitted do not.
	code, body := hreq(t, "POST", u1+"/api/v1/batches",
		map[string]any{"expectedChunks": 2, "protectWrites": true}, "")
	if code != 201 {
		t.Fatalf("protected create: code=%d body=%v", code, body)
	}
	pid := body["batchId"].(string)
	token, _ := body["writeToken"].(string)
	if len(token) != 43 {
		t.Fatalf("protected create must issue a 43-char base64url token: %v", body)
	}

	code, body = hreq(t, "POST", u2+"/api/v1/batches",
		map[string]any{"expectedChunks": 2, "protectWrites": false}, "")
	if code != 201 {
		t.Fatalf("explicit false create: %d %v", code, body)
	}
	if _, present := body["writeToken"]; present {
		t.Fatalf("protectWrites:false must not return a writeToken: %v", body)
	}
	code, body = hreq(t, "POST", u1+"/api/v1/batches",
		map[string]int{"expectedChunks": 2}, "")
	if code != 201 {
		t.Fatalf("omitted create: %d %v", code, body)
	}
	if _, present := body["writeToken"]; present {
		t.Fatalf("omitted protectWrites must not return a writeToken: %v", body)
	}

	// Status reads never expose a token or digest, on either instance.
	for _, base := range []string{u1, u2} {
		code, snap := hreq(t, "GET", base+"/api/v1/batches/"+pid, nil, "")
		if code != 200 {
			t.Fatalf("status: %d %v", code, snap)
		}
		if _, leak := snap["writeToken"]; leak {
			t.Fatalf("status response leaked writeToken: %v", snap)
		}
		if _, leak := snap["writeTokenDigest"]; leak {
			t.Fatalf("status response leaked digest: %v", snap)
		}
	}

	chunkURL := u2 + "/api/v1/batches/" + pid + "/chunks"
	sealURL := u1 + "/api/v1/batches/" + pid + "/seal"

	probe := func(name string, code int, b map[string]any) {
		t.Helper()
		if code != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
			t.Fatalf("%s: code=%d body=%v, want 403 WRITE_CAPABILITY_REQUIRED", name, code, b)
		}
	}

	// State OPEN/empty: missing and wrong tokens are identical 403.
	code, b := hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "a"}, "")
	probe("open chunk missing token", code, b)
	openMissing := b
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "a"}, "totally-wrong")
	probe("open chunk wrong token", code, b)

	// The rejected request changed nothing.
	code, snap := hreq(t, "GET", u1+"/api/v1/batches/"+pid, nil, "")
	if int(snap["received"].(float64)) != 0 {
		t.Fatalf("rejected write mutated state: %v", snap)
	}

	// Authenticated first write and duplicate.
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "a"}, token)
	if code != 201 || b["duplicate"] != false {
		t.Fatalf("authenticated first write: %d %v", code, b)
	}
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "a"}, token)
	if code != 200 || b["duplicate"] != true {
		t.Fatalf("authenticated duplicate: %d %v", code, b)
	}

	// OPEN with one chunk: conflicting payload without token is the uniform
	// 403, never CHUNK_CONFLICT; seal is 403 not INCOMPLETE.
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "DIFFERENT"}, "")
	probe("conflict probe missing token", code, b)
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "DIFFERENT"}, "wrong")
	probe("conflict probe wrong token", code, b)
	code, b = hreq(t, "POST", sealURL, nil, "")
	probe("incomplete seal missing token", code, b)
	incompleteMissing := b
	code, b = hreq(t, "POST", sealURL, nil, "wrong")
	probe("incomplete seal wrong token", code, b)

	// Complete and seal with the token.
	if code, _ = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 2, "payload": "b"}, token); code != 201 {
		t.Fatalf("authenticated second write: %d", code)
	}
	code, sealed := hreq(t, "POST", sealURL, nil, token)
	if code != 200 || sealed["status"] != "SEALED" {
		t.Fatalf("authenticated seal: %d %v", code, sealed)
	}

	// State SEALED: same uniform 403, never idempotent 200.
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "a"}, "")
	probe("sealed identical chunk missing token", code, b)
	sealedMissing := b
	code, b = hreq(t, "POST", sealURL, nil, "")
	probe("sealed reseal missing token", code, b)

	// The 403 envelope must be identical in every state.
	if openMissing["message"] != incompleteMissing["message"] ||
		openMissing["message"] != sealedMissing["message"] {
		t.Fatalf("403 messages differ across states: %q %q %q",
			openMissing["message"], incompleteMissing["message"], sealedMissing["message"])
	}

	// Token holders still get idempotent sealed verdicts.
	code, b = hreq(t, "POST", chunkURL,
		map[string]any{"seq": 1, "payload": "a"}, token)
	if code != 200 || b["duplicate"] != true {
		t.Fatalf("authenticated sealed retransmit: %d %v", code, b)
	}
}

// TestHTTPRotateWriteToken exercises the rotate endpoint matrix across two
// instances and cross-instance visibility of the rotated token.
func TestHTTPRotateWriteToken(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()

	rotateURL := func(base, id string) string {
		return base + "/api/v1/batches/" + id + "/write-token/rotate"
	}
	chunkURL := func(base, id string) string {
		return base + "/api/v1/batches/" + id + "/chunks"
	}

	// Unprotected -> 409 BATCH_NOT_PROTECTED, with or without a header.
	_, body := hreq(t, "POST", u1+"/api/v1/batches", map[string]int{"expectedChunks": 1}, "")
	uid := body["batchId"].(string)
	code, b := hreq(t, "POST", rotateURL(u1, uid), nil, "")
	if code != 409 || b["error"] != "BATCH_NOT_PROTECTED" {
		t.Fatalf("rotate unprotected: %d %v", code, b)
	}
	code, b = hreq(t, "POST", rotateURL(u2, uid), nil, "junk")
	if code != 409 || b["error"] != "BATCH_NOT_PROTECTED" {
		t.Fatalf("rotate unprotected with header: %d %v", code, b)
	}
	// Missing batch -> 404.
	code, b = hreq(t, "POST",
		rotateURL(u1, "ffffffffffffffffffffffffffffffff"), nil, "x")
	if code != 404 || b["error"] != "BATCH_NOT_FOUND" {
		t.Fatalf("rotate missing: %d %v", code, b)
	}

	_, body = hreq(t, "POST", u1+"/api/v1/batches",
		map[string]any{"expectedChunks": 1, "protectWrites": true}, "")
	pid := body["batchId"].(string)
	token := body["writeToken"].(string)

	code, b = hreq(t, "POST", rotateURL(u1, pid), nil, "")
	if code != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
		t.Fatalf("rotate missing token: %d %v", code, b)
	}
	code, b = hreq(t, "POST", rotateURL(u2, pid), nil, "wrong")
	if code != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
		t.Fatalf("rotate wrong token: %d %v", code, b)
	}

	code, rot := hreq(t, "POST", rotateURL(u2, pid), nil, token)
	if code != 200 {
		t.Fatalf("rotate: %d %v", code, rot)
	}
	newToken, _ := rot["writeToken"].(string)
	if len(newToken) != 43 || newToken == token {
		t.Fatalf("rotated token bad: old=%q new=%v", token, rot["writeToken"])
	}
	if rot["batchId"] != pid {
		t.Fatalf("rotate body missing batchId: %v", rot)
	}

	// Old token dead on both instances.
	if code, _ = hreq(t, "POST", chunkURL(u1, pid),
		map[string]any{"seq": 1, "payload": "x"}, token); code != 403 {
		t.Fatalf("old token write after rotate: %d", code)
	}
	if code, _ = hreq(t, "POST", rotateURL(u1, pid), nil, token); code != 403 {
		t.Fatalf("old token rotate after rotate: %d", code)
	}
	// New token works cross-instance.
	if code, _ = hreq(t, "POST", chunkURL(u1, pid),
		map[string]any{"seq": 1, "payload": "x"}, newToken); code != 201 {
		t.Fatalf("new token write: %d", code)
	}
	// Rotate again; newest credential must win and the previous one die.
	code, rot2 := hreq(t, "POST", rotateURL(u2, pid), nil, newToken)
	if code != 200 {
		t.Fatalf("second rotate: %d %v", code, rot2)
	}
	newest := rot2["writeToken"].(string)
	if code, _ = hreq(t, "POST", chunkURL(u2, pid),
		map[string]any{"seq": 1, "payload": "x"}, newToken); code != 403 {
		t.Fatalf("previous token must fail after second rotate: %d", code)
	}
	if code, _ = hreq(t, "POST", u1+"/api/v1/batches/"+pid+"/seal", nil, newest); code != 200 {
		t.Fatalf("newest token seal: %d", code)
	}
}

// --- forced-interleaving support ---

type rawFixture struct {
	pool *pgxpool.Pool
}

func newRawFixture(t *testing.T, schema string) *rawFixture {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testsupport.SchemaURL(testsupport.RequireURL(t), schema))
	if err != nil {
		t.Fatalf("raw pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("raw pool ping: %v", err)
	}
	t.Cleanup(pool.Close)
	return &rawFixture{pool: pool}
}

func randomTokenDigest(t *testing.T) (token string, digest []byte) {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	return token, sum[:]
}

// lockHolder opens a raw transaction holding the batches row FOR UPDATE.
func (fx *rawFixture) lockHolder(t *testing.T, batchID string) (pgx.Tx, uint64) {
	t.Helper()
	ctx := context.Background()
	tx, err := fx.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(ctx,
		`SELECT write_token_digest FROM batches WHERE id = $1 FOR UPDATE`,
		batchID); err != nil {
		t.Fatalf("lock: %v", err)
	}
	var xid uint64
	if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&xid); err != nil {
		t.Fatalf("xid: %v", err)
	}
	return tx, xid
}

// waitBlocked polls pg_locks until a transaction waits on the holder xid.
func (fx *rawFixture) waitBlocked(t *testing.T, xid uint64) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := fx.pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_locks
			 WHERE locktype = 'transactionid' AND NOT granted
			   AND transactionid::text = $1`, fmt.Sprint(xid)).Scan(&n); err != nil {
			t.Fatalf("poll locks: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no waiter queued on xid %d within 10s", xid)
}

// TestHTTPRotationFirstQueuedSubmitOldTokenFails forces the ordering in
// which a rotation holds the batch lock and commits while an old-token
// submit (through a live API instance) is queued. After commit the submit
// is granted the lock, observes the new digest, returns 403 and writes
// nothing.
func TestHTTPRotationFirstQueuedSubmitOldTokenFails(t *testing.T) {
	ctx := context.Background()
	base := testsupport.RequireURL(t)
	schema := testsupport.IsolatedSchema(ctx, t)
	defer testsupport.DropSchema(ctx, schema)

	s1, err := store.New(ctx, testsupport.SchemaURL(base, schema))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := store.New(ctx, testsupport.SchemaURL(base, schema))
	if err != nil {
		t.Fatal(err)
	}
	srv1 := httptest.NewServer(api.NewServer(s1, nil).Handler())
	srv2 := httptest.NewServer(api.NewServer(s2, nil).Handler())
	defer func() { srv1.Close(); srv2.Close(); s1.Close(); s2.Close() }()
	u1, u2 := srv1.URL, srv2.URL
	fx := newRawFixture(t, schema)

	_, pb := hreq(t, "POST", u1+"/api/v1/batches",
		map[string]any{"expectedChunks": 1, "protectWrites": true}, "")
	id := pb["batchId"].(string)
	old := pb["writeToken"].(string)

	// Holder acts as the rotation verdict on the other instance, uncommitted.
	holder, xid := fx.lockHolder(t, id)
	newToken, newDigest := randomTokenDigest(t)
	if _, err := holder.Exec(ctx,
		`UPDATE batches SET write_token_digest = $1 WHERE id = $2`,
		newDigest, id); err != nil {
		t.Fatalf("rotation verdict: %v", err)
	}

	type result struct {
		code int
		body map[string]any
	}
	done := make(chan result, 1)
	go func() {
		c, b := hreq(t, "POST", u2+"/api/v1/batches/"+id+"/chunks",
			map[string]any{"seq": 1, "payload": "late"}, old)
		done <- result{c, b}
	}()
	fx.waitBlocked(t, xid)

	if err := holder.Commit(ctx); err != nil {
		t.Fatalf("commit rotation: %v", err)
	}
	r := <-done
	if r.code != 403 || r.body["error"] != "WRITE_CAPABILITY_REQUIRED" {
		t.Fatalf("queued old-token submit after rotation: %d %v", r.code, r.body)
	}

	code, snap := hreq(t, "GET", u1+"/api/v1/batches/"+id, nil, "")
	if code != 200 || int(snap["received"].(float64)) != 0 {
		t.Fatalf("queued rejected submit wrote data: %d %v", code, snap)
	}
	// New token is the sole capability and is accepted on instance 1.
	code, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 1, "payload": "now"}, newToken)
	if code != 201 {
		t.Fatalf("new token write after forced rotation: %d %v", code, b)
	}
}

// TestHTTPSubmitFirstSucceedsThenOldTokenFails forces the reverse order: an
// authenticated old-token submit holds the lock and commits first (so it
// succeeds), a rotation queued behind it then succeeds, and immediately
// afterwards the old token is invalid on both instances.
func TestHTTPSubmitFirstSucceedsThenOldTokenFails(t *testing.T) {
	ctx := context.Background()
	base := testsupport.RequireURL(t)
	schema := testsupport.IsolatedSchema(ctx, t)
	defer testsupport.DropSchema(ctx, schema)

	s1, err := store.New(ctx, testsupport.SchemaURL(base, schema))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := store.New(ctx, testsupport.SchemaURL(base, schema))
	if err != nil {
		t.Fatal(err)
	}
	srv1 := httptest.NewServer(api.NewServer(s1, nil).Handler())
	srv2 := httptest.NewServer(api.NewServer(s2, nil).Handler())
	defer func() { srv1.Close(); srv2.Close(); s1.Close(); s2.Close() }()
	u1, u2 := srv1.URL, srv2.URL
	fx := newRawFixture(t, schema)

	_, pb := hreq(t, "POST", u1+"/api/v1/batches",
		map[string]any{"expectedChunks": 2, "protectWrites": true}, "")
	id := pb["batchId"].(string)
	old := pb["writeToken"].(string)

	// Holder = in-flight authenticated submit, chunk inserted but uncommitted.
	holder, xid := fx.lockHolder(t, id)
	if _, err := holder.Exec(ctx,
		`INSERT INTO chunks (batch_id, seq, payload) VALUES ($1, 1, 'first')`,
		id); err != nil {
		t.Fatalf("submit verdict: %v", err)
	}

	type result struct {
		code int
		body map[string]any
	}
	done := make(chan result, 1)
	go func() {
		c, b := hreq(t, "POST",
			u2+"/api/v1/batches/"+id+"/write-token/rotate", nil, old)
		done <- result{c, b}
	}()
	fx.waitBlocked(t, xid)

	// Submit commits first; the queued rotation then observes the old digest.
	if err := holder.Commit(ctx); err != nil {
		t.Fatalf("commit submit: %v", err)
	}
	r := <-done
	if r.code != 200 {
		t.Fatalf("queued rotation must succeed after submit commit: %d %v", r.code, r.body)
	}
	newToken := r.body["writeToken"].(string)

	code, snap := hreq(t, "GET", u1+"/api/v1/batches/"+id, nil, "")
	if code != 200 || int(snap["received"].(float64)) != 1 {
		t.Fatalf("first submit should have landed first: %d %v", code, snap)
	}
	if code, _ = hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 2, "payload": "second"}, old); code != 403 {
		t.Fatalf("old token must fail after rotation: %d", code)
	}
	if code, _ = hreq(t, "POST", u2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 2, "payload": "second"}, newToken); code != 201 {
		t.Fatalf("new token must finish batch: %d", code)
	}
	code, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/seal", nil, newToken)
	if code != 200 || b["status"] != "SEALED" {
		t.Fatalf("seal with rotated token: %d %v", code, b)
	}
}

// TestHTTPCrossInstanceRotationRaces fires rotations and writes at one
// protected batch from both instances for many rounds. Post-conditions are
// order-independent: unauthenticated writes never land, exactly one of two
// concurrent rotations succeeds, and the token it returns is the only
// credential active afterwards.
func TestHTTPCrossInstanceRotationRaces(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()

	const rounds = 20
	for i := 0; i < rounds; i++ {
		_, body := hreq(t, "POST", u1+"/api/v1/batches",
			map[string]any{"expectedChunks": 1, "protectWrites": true}, "")
		id := body["batchId"].(string)
		token := body["writeToken"].(string)

		var wg sync.WaitGroup
		wg.Add(4)
		var rot1, rot2 string
		var rc1, rc2 int
		go func() {
			defer wg.Done()
			c, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/write-token/rotate", nil, token)
			rc1 = c
			if c == 200 {
				rot1 = b["writeToken"].(string)
			}
		}()
		go func() {
			defer wg.Done()
			c, b := hreq(t, "POST", u2+"/api/v1/batches/"+id+"/write-token/rotate", nil, token)
			rc2 = c
			if c == 200 {
				rot2 = b["writeToken"].(string)
			}
		}()
		go func() {
			defer wg.Done()
			_, _ = hreq(t, "POST", u2+"/api/v1/batches/"+id+"/chunks",
				map[string]any{"seq": 1, "payload": "old"}, token)
		}()
		go func() {
			defer wg.Done()
			c, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
				map[string]any{"seq": 1, "payload": "none"}, "")
			if c != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
				t.Errorf("round %d: unauthenticated write got %d %v", i, c, b)
			}
		}()
		wg.Wait()

		if (rc1 == 200) == (rc2 == 200) {
			t.Fatalf("round %d: exactly one concurrent rotation must succeed, codes %d/%d", i, rc1, rc2)
		}
		winner := rot1
		if winner == "" {
			winner = rot2
		}

		code, snap := hreq(t, "GET", u1+"/api/v1/batches/"+id, nil, "")
		if code != 200 || int(snap["received"].(float64)) > 1 {
			t.Fatalf("round %d: bad snapshot %d %v", i, code, snap)
		}
		// The winning token is the active credential: another rotation with
		// it must return 200.
		if code, _ = hreq(t, "POST", u2+"/api/v1/batches/"+id+"/write-token/rotate", nil, winner); code != 200 {
			t.Fatalf("round %d: winning token not active afterwards: %d", i, code)
		}
	}
}

// TestHTTPProtectedWrongTokenIdenticalAcrossStates verifies wrong/missing
// tokens produce the identical 403 for chunk, seal and rotate while the
// batch is open/empty, incomplete and sealed.
func TestHTTPProtectedWrongTokenIdenticalAcrossStates(t *testing.T) {
	u1, u2, cleanup := newAPIs(t)
	defer cleanup()

	_, body := hreq(t, "POST", u1+"/api/v1/batches",
		map[string]any{"expectedChunks": 2, "protectWrites": true}, "")
	id := body["batchId"].(string)
	token := body["writeToken"].(string)

	tryAll := func(state string) {
		t.Helper()
		for _, tok := range []string{"", "wrong"} {
			for _, kind := range []string{"chunk", "seal", "rotate"} {
				var code int
				var b map[string]any
				switch kind {
				case "chunk":
					code, b = hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
						map[string]any{"seq": 1, "payload": "a"}, tok)
				case "seal":
					code, b = hreq(t, "POST", u2+"/api/v1/batches/"+id+"/seal", nil, tok)
				case "rotate":
					code, b = hreq(t, "POST", u1+"/api/v1/batches/"+id+"/write-token/rotate", nil, tok)
				}
				if code != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
					t.Fatalf("%s/%s token=%q: %d %v", state, kind, tok, code, b)
				}
			}
		}
	}

	tryAll("open")
	// seq < 1 is only judged after the token verdict: a missing token on a
	// protected batch is still the uniform 403, never 400 INVALID_SEQ.
	if code, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 0, "payload": "a"}, ""); code != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
		t.Fatalf("protected seq=0 without token: %d %v", code, b)
	}
	if code, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 0, "payload": "a"}, token); code != 400 || b["error"] != "INVALID_SEQ" {
		t.Fatalf("protected seq=0 with valid token: %d %v want 400 INVALID_SEQ", code, b)
	}
	if code, _ := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 1, "payload": "a"}, token); code != 201 {
		t.Fatalf("seed chunk 1: %d", code)
	}
	tryAll("incomplete")
	if code, _ := hreq(t, "POST", u2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 2, "payload": "b"}, token); code != 201 {
		t.Fatalf("seed chunk 2: %d", code)
	}
	if code, b := hreq(t, "POST", u1+"/api/v1/batches/"+id+"/seal", nil, token); code != 200 || b["status"] != "SEALED" {
		t.Fatalf("seed seal: %d %v", code, b)
	}
	tryAll("sealed")
}
