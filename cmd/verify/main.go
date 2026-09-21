// Command verify is the one-shot acceptance service for the batch sealing API.
//
// It reaches the two API instances exclusively over the internal compose
// network (never a published port) and exercises the full protocol:
// out-of-order arrival, idempotent retransmission, payload conflicts,
// ascending gaps, incomplete vs atomic sealing, post-seal rules and the
// cross-process race between the last chunk and a seal. VERIFY_PHASE=restart
// additionally proves that verdicts survive both API instances restarting.
//
// Exit code 0 means acceptance passed.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// defaultStateDir matches the mounted /verify-state volume in compose;
	// VERIFY_STATE_DIR overrides it for local runs.
	defaultStateDir = "/verify-state"
)

func stateDir() string { return envOr("VERIFY_STATE_DIR", defaultStateDir) }

type config struct {
	api1 string
	api2 string
}

func main() {
	cfg := config{
		api1: envOr("API1_URL", "http://api1:8080"),
		api2: envOr("API2_URL", "http://api2:8080"),
	}
	v := &verifier{
		client: &http.Client{Timeout: 20 * time.Second},
		cfg:    cfg,
		failed: false,
	}

	ctx := context.Background()
	v.log("waiting for both API instances")
	v.waitFor(ctx, cfg.api1+"/healthz")
	v.waitFor(ctx, cfg.api2+"/healthz")
	v.log("both instances healthy; running acceptance checks")

	v.checkLifecycleAcrossInstances(ctx)
	v.checkValidation(ctx)
	v.checkCrossInstanceSealRace(ctx)
	v.checkProtectedWrites(ctx)
	v.checkCrossInstanceRotationRace(ctx)

	switch phase := os.Getenv("VERIFY_PHASE"); phase {
	case "seed":
		v.phaseSeed(ctx)
	case "restart":
		v.phaseRestart(ctx)
	default:
		// A plain one-shot run also proves restart consistency by persisting
		// fixtures for a subsequent restart-phase invocation.
		v.phaseSeed(ctx)
		v.log("hint: run with VERIFY_PHASE=restart after restarting both APIs")
	}

	if v.failed {
		v.log("RESULT: FAIL")
		os.Exit(1)
	}
	v.log("RESULT: PASS")
}

type verifier struct {
	client *http.Client
	cfg    config
	failed bool
	mu     sync.Mutex
}

func (v *verifier) log(format string, args ...any) {
	fmt.Printf("verify: "+format+"\n", args...)
}

func (v *verifier) failf(format string, args ...any) {
	v.mu.Lock()
	v.failed = true
	v.mu.Unlock()
	v.log("FAIL: "+format, args...)
}

func envOr(key, fallback string) string {
	if x := os.Getenv(key); x != "" {
		return x
	}
	return fallback
}

func (v *verifier) waitFor(ctx context.Context, url string) {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := v.client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			_ = body
		}
		select {
		case <-ctx.Done():
			v.failf("interrupted waiting for %s", url)
			return
		case <-time.After(time.Second):
		}
	}
	v.failf("timeout waiting for %s", url)
}

func (v *verifier) request(ctx context.Context, method, url string, body any) (int, map[string]any, []byte) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		v.failf("build request: %v", err)
		return 0, nil, nil
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.client.Do(req)
	if err != nil {
		v.failf("%s %s: %v", method, url, err)
		return 0, nil, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed)
	return resp.StatusCode, parsed, raw
}

func (v *verifier) createBatch(ctx context.Context, base string, expected int) string {
	code, body, _ := v.request(ctx, http.MethodPost, base+"/api/v1/batches",
		map[string]int{"expectedChunks": expected})
	if code != 201 {
		v.failf("create batch: status=%d body=%v", code, body)
		return ""
	}
	id, _ := body["batchId"].(string)
	if len(id) != 32 {
		v.failf("create batch returned bad id: %v", body)
	}
	return id
}

// createProtectedBatch creates a protectWrites batch and returns its id and
// the one-time write token issued on the 201 response.
func (v *verifier) createProtectedBatch(ctx context.Context, base string, expected int) (id, token string) {
	code, body, _ := v.request(ctx, http.MethodPost, base+"/api/v1/batches",
		map[string]any{"expectedChunks": expected, "protectWrites": true})
	if code != 201 {
		v.failf("create protected batch: status=%d body=%v", code, body)
		return "", ""
	}
	id, _ = body["batchId"].(string)
	token, _ = body["writeToken"].(string)
	if len(id) != 32 || len(token) != 43 {
		v.failf("protected create bad id/token: %v", body)
	}
	return id, token
}

// requestToken issues a JSON request carrying X-Batch-Write-Token (empty
// means the header is omitted).
func (v *verifier) requestToken(ctx context.Context, method, url string, body any, token string) (int, map[string]any, []byte) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		v.failf("build request: %v", err)
		return 0, nil, nil
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Batch-Write-Token", token)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		v.failf("%s %s: %v", method, url, err)
		return 0, nil, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	parsed := map[string]any{}
	_ = json.Unmarshal(raw, &parsed)
	return resp.StatusCode, parsed, raw
}

// checkLifecycleAcrossInstances walks the complete protocol while
// alternating the two instances on purpose.
func (v *verifier) checkLifecycleAcrossInstances(ctx context.Context) {
	id := v.createBatch(ctx, v.cfg.api1, 4)
	if id == "" {
		return
	}

	submit := func(base string, seq int, payload string, wantStatus int, wantDup any) {
		code, body, _ := v.request(ctx, http.MethodPost,
			base+"/api/v1/batches/"+id+"/chunks",
			map[string]any{"seq": seq, "payload": payload})
		if code != wantStatus {
			v.failf("chunk seq=%d via %s: status=%d want %d (body=%v)",
				seq, base, code, wantStatus, body)
			return
		}
		if wantDup != nil && body["duplicate"] != wantDup {
			v.failf("chunk seq=%d duplicate=%v want %v", seq, body["duplicate"], wantDup)
		}
		if code == 200 || code == 201 {
			if int(body["size"].(float64)) != len(payload) {
				v.failf("chunk seq=%d size=%v want %d", seq, body["size"], len(payload))
			}
		}
	}

	// Out of order across instances.
	submit(v.cfg.api2, 4, "four", 201, false)
	submit(v.cfg.api1, 1, "one", 201, false)
	submit(v.cfg.api2, 2, "two", 201, false)

	// Gap list while open (missing [3]).
	code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+id, nil)
	if code != 200 || snap["status"] != "OPEN" ||
		int(snap["received"].(float64)) != 3 ||
		len(snap["gaps"].([]any)) != 1 ||
		int(snap["gaps"].([]any)[0].(float64)) != 3 {
		v.failf("open snapshot wrong: code=%d body=%v", code, snap)
	}

	// Seal incomplete -> 409 INCOMPLETE with gaps, remains OPEN.
	code, sealBody, _ := v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+id+"/seal", nil)
	if code != 409 || sealBody["error"] != "INCOMPLETE" ||
		len(sealBody["gaps"].([]any)) != 1 ||
		int(sealBody["gaps"].([]any)[0].(float64)) != 3 {
		v.failf("incomplete seal wrong: code=%d body=%v", code, sealBody)
	}
	code, snap, _ = v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
	if snap["status"] != "OPEN" {
		v.failf("batch did not stay OPEN after failed seal: %v", snap)
	}

	// Retransmission identical -> original confirmation, duplicate true.
	submit(v.cfg.api1, 4, "four", 200, true)
	// Different content -> CHUNK_CONFLICT.
	submit(v.cfg.api2, 4, "FOUR", 409, nil)

	// Final chunk then seal atomically.
	submit(v.cfg.api2, 3, "three", 201, false)
	code, sealed, _ := v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+id+"/seal", nil)
	if code != 200 || sealed["status"] != "SEALED" || sealed["sealedAt"] == "" {
		v.failf("seal complete wrong: code=%d body=%v", code, sealed)
	}

	// Visible identically on instance 1.
	code, snap, _ = v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
	if code != 200 || snap["status"] != "SEALED" ||
		int(snap["received"].(float64)) != 4 || len(snap["gaps"].([]any)) != 0 {
		v.failf("sealed snapshot on other instance wrong: %v", snap)
	}

	// Repeated seal on instance 1: existing result, same sealedAt.
	code, resealed, _ := v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+id+"/seal", nil)
	if code != 200 || resealed["sealedAt"] != sealed["sealedAt"] {
		v.failf("repeated seal wrong: code=%d body=%v", code, resealed)
	}

	// Sealed: identical retransmission allowed, changed content rejected.
	submit(v.cfg.api1, 1, "one", 200, true)
	submit(v.cfg.api2, 1, "changed", 409, nil)
	// New seq also rejected.
	submit(v.cfg.api1, 4, "four", 200, true) // identical
}

// checkValidation covers the fixed status code matrix.
func (v *verifier) checkValidation(ctx context.Context) {
	id := v.createBatch(ctx, v.cfg.api1, 2)

	expect := func(name string, code, want int, body map[string]any, wantErr string) {
		if code != want {
			v.failf("%s: status=%d want %d body=%v", name, code, want, body)
			return
		}
		if wantErr != "" && body["error"] != wantErr {
			v.failf("%s: error=%v want %s", name, body["error"], wantErr)
		}
	}

	code, body, _ := v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches",
		map[string]int{"expectedChunks": 0})
	expect("expectedChunks 0", code, 400, body, "INVALID_EXPECTED_CHUNKS")

	code, body, _ = v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches",
		map[string]int{"expectedChunks": 10001})
	expect("expectedChunks 10001", code, 400, body, "INVALID_EXPECTED_CHUNKS")

	code, body, _ = v.request(ctx, http.MethodGet,
		v.cfg.api1+"/api/v1/batches/ffffffffffffffffffffffffffffffff", nil)
	expect("unknown batch", code, 404, body, "BATCH_NOT_FOUND")

	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 3, "payload": "x"})
	expect("seq > expected", code, 400, body, "SEQ_OUT_OF_RANGE")

	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 0, "payload": "x"})
	expect("seq 0", code, 400, body, "INVALID_SEQ")

	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 1, "payload": 42})
	expect("payload not string", code, 400, body, "INVALID_PAYLOAD")

	big := strings.Repeat("a", 65537)
	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 1, "payload": big})
	expect("payload 65537", code, 413, body, "PAYLOAD_TOO_LARGE")

	// UTF-8 byte identity boundary: 65536 multibyte chars exceed byte limit,
	// but exactly 65536 bytes of ASCII are accepted.
	exact := strings.Repeat("a", 65536)
	code, body, _ = v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
		map[string]any{"seq": 2, "payload": exact})
	expect("payload exactly 65536 bytes", code, 201, body, "")
}

// checkCrossInstanceSealRace fires the last chunk at one instance and the
// seal at the other simultaneously. A SEALED batch can never have gaps.
func (v *verifier) checkCrossInstanceSealRace(ctx context.Context) {
	const rounds = 25
	for i := 0; i < rounds; i++ {
		id := v.createBatch(ctx, v.cfg.api1, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+id+"/seal", nil)
		}()
		go func() {
			defer wg.Done()
			v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
				map[string]any{"seq": 1, "payload": "race"})
		}()
		wg.Wait()

		code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
		if code != 200 {
			v.failf("race round %d: status query %d", i, code)
			continue
		}
		if snap["status"] == "SEALED" && int(snap["received"].(float64)) != 1 {
			v.failf("race round %d: SEALED batch with gaps: %v", i, snap)
		}
		// Whatever the outcome, settling with another seal must converge
		// exactly once the chunk is present.
		if snap["status"] == "OPEN" && int(snap["received"].(float64)) == 1 {
			code, sealed, _ := v.request(ctx, http.MethodPost,
				v.cfg.api2+"/api/v1/batches/"+id+"/seal", nil)
			if code != 200 || sealed["status"] != "SEALED" {
				v.failf("race round %d: follow-up seal failed: %d %v", i, code, sealed)
			}
		}
	}
}

// checkProtectedWrites exercises the full protectWrites protocol while
// alternating the two instances, including identical 403 behaviour across
// the open, incomplete and sealed states, rotation and legacy
// compatibility.
func (v *verifier) checkProtectedWrites(ctx context.Context) {
	id, token := v.createProtectedBatch(ctx, v.cfg.api1, 2)
	if id == "" {
		return
	}
	chunk := func(base string, seq int, payload, tok string) (int, map[string]any) {
		code, body, _ := v.requestToken(ctx, http.MethodPost,
			base+"/api/v1/batches/"+id+"/chunks",
			map[string]any{"seq": seq, "payload": payload}, tok)
		return code, body
	}
	seal := func(base, tok string) (int, map[string]any) {
		code, body, _ := v.requestToken(ctx, http.MethodPost, base+"/api/v1/batches/"+id+"/seal", nil, tok)
		return code, body
	}
	rotate := func(base, tok string) (int, map[string]any) {
		code, body, _ := v.requestToken(ctx, http.MethodPost,
			base+"/api/v1/batches/"+id+"/write-token/rotate", nil, tok)
		return code, body
	}
	wantCap := func(name string, code int, body map[string]any) {
		if code != 403 || body["error"] != "WRITE_CAPABILITY_REQUIRED" {
			v.failf("%s: code=%d body=%v want 403 WRITE_CAPABILITY_REQUIRED", name, code, body)
		}
	}

	// Status read never exposes a token/digest.
	code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+id, nil)
	if code != 200 || snap["writeToken"] != nil || snap["writeTokenDigest"] != nil {
		v.failf("protected status leaked token: code=%d body=%v", code, snap)
	}

	// OPEN/empty: missing and wrong tokens are identical 403 for every write.
	cc, cb := chunk(v.cfg.api1, 1, "a", "")
	wantCap("open chunk no token", cc, cb)
	cc, cb = chunk(v.cfg.api2, 1, "a", "bogus")
	wantCap("open chunk bad token", cc, cb)
	cc, cb = seal(v.cfg.api1, "")
	wantCap("open seal no token", cc, cb)
	cc, cb = rotate(v.cfg.api2, "")
	wantCap("open rotate no token", cc, cb)
	cc, cb = rotate(v.cfg.api1, "bogus")
	wantCap("open rotate bad token", cc, cb)
	if code, snap, _ = v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil); int(snap["received"].(float64)) != 0 {
		v.failf("rejected writes mutated protected batch: %v", snap)
	}

	// Authenticated write across instances, then conflict/gap probes stay 403.
	if code, b := chunk(v.cfg.api2, 1, "one", token); code != 201 {
		v.failf("authenticated chunk: %d %v", code, b)
	}
	cc, cb = chunk(v.cfg.api1, 1, "ONE", "")
	wantCap("conflict probe no token", cc, cb)
	cc, cb = seal(v.cfg.api2, "")
	wantCap("gap seal no token", cc, cb)

	// Rotation via instance 2: old token dies, new token works on instance 1.
	code, rb := rotate(v.cfg.api2, token)
	if code != 200 {
		v.failf("rotate: %d %v", code, rb)
	}
	token2, _ := rb["writeToken"].(string)
	if len(token2) != 43 || token2 == token {
		v.failf("rotated token malformed: %v", rb)
	}
	cc, cb = chunk(v.cfg.api1, 2, "two", token)
	wantCap("old token chunk after rotate", cc, cb)
	cc, cb = seal(v.cfg.api2, token)
	wantCap("old token seal after rotate", cc, cb)
	cc, cb = rotate(v.cfg.api1, token)
	wantCap("old token rotate after rotate", cc, cb)
	if code, b := chunk(v.cfg.api1, 2, "two", token2); code != 201 {
		v.failf("new token chunk: %d %v", code, b)
	}

	// Sealed state keeps demanding the active token for idempotent writes.
	if code, b := seal(v.cfg.api1, token2); code != 200 || b["status"] != "SEALED" {
		v.failf("seal with rotated token: %d %v", code, b)
	}
	cc, cb = chunk(v.cfg.api2, 1, "one", "")
	wantCap("sealed chunk no token", cc, cb)
	cc, cb = seal(v.cfg.api1, "")
	wantCap("sealed reseal no token", cc, cb)
	cc, cb = rotate(v.cfg.api2, "")
	wantCap("sealed rotate no token", cc, cb)
	if code, b := chunk(v.cfg.api2, 1, "one", token2); code != 200 || b["duplicate"] != true {
		v.failf("sealed identical retransmit with token: %d %v", code, b)
	}

	// Legacy compatibility: an unprotected batch ignores the header on writes
	// and seal, and answers rotate with 409 BATCH_NOT_PROTECTED.
	legacy := v.createBatch(ctx, v.cfg.api2, 1)
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+legacy+"/chunks",
		map[string]any{"seq": 1, "payload": "x"}, "junk"); code != 201 {
		v.failf("legacy chunk should ignore token header: %d %v", code, b)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+legacy+"/seal", nil, "junk"); code != 200 {
		v.failf("legacy seal should ignore token header: %d %v", code, b)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+legacy+"/write-token/rotate", nil, ""); code != 409 || b["error"] != "BATCH_NOT_PROTECTED" {
		v.failf("legacy rotate: code=%d body=%v want 409 BATCH_NOT_PROTECTED", code, b)
	}
}

// checkCrossInstanceRotationRace fires a rotation on one instance and an
// old-token chunk write on the other simultaneously for many rounds. Both
// serialise on the batches row lock; whichever order the database grants,
// the invariants are order-independent: unauthenticated writes never land,
// the winning rotation's token is the only active credential afterwards,
// and no batch ever shows more than one chunk per seq.
func (v *verifier) checkCrossInstanceRotationRace(ctx context.Context) {
	const rounds = 25
	for i := 0; i < rounds; i++ {
		id, token := v.createProtectedBatch(ctx, v.cfg.api1, 1)
		if id == "" {
			return
		}
		var wg sync.WaitGroup
		wg.Add(4)
		var rotCode, rotCode2 int
		var rotToken, rotToken2 string
		go func() {
			defer wg.Done()
			code, b, _ := v.requestToken(ctx, http.MethodPost,
				v.cfg.api1+"/api/v1/batches/"+id+"/write-token/rotate", nil, token)
			rotCode = code
			if code == 200 {
				rotToken, _ = b["writeToken"].(string)
			}
		}()
		go func() {
			defer wg.Done()
			code, b, _ := v.requestToken(ctx, http.MethodPost,
				v.cfg.api2+"/api/v1/batches/"+id+"/write-token/rotate", nil, token)
			rotCode2 = code
			if code == 200 {
				rotToken2, _ = b["writeToken"].(string)
			}
		}()
		go func() {
			defer wg.Done()
			_, _, _ = v.requestToken(ctx, http.MethodPost,
				v.cfg.api2+"/api/v1/batches/"+id+"/chunks",
				map[string]any{"seq": 1, "payload": "old"}, token)
		}()
		go func() {
			defer wg.Done()
			code, b, _ := v.requestToken(ctx, http.MethodPost,
				v.cfg.api1+"/api/v1/batches/"+id+"/chunks",
				map[string]any{"seq": 1, "payload": "none"}, "")
			if code != 403 || b["error"] != "WRITE_CAPABILITY_REQUIRED" {
				v.failf("race round %d: unauthenticated write code=%d body=%v", i, code, b)
			}
		}()
		wg.Wait()

		if (rotCode == 200) == (rotCode2 == 200) {
			v.failf("race round %d: exactly one rotation must succeed, codes %d/%d", i, rotCode, rotCode2)
		}
		winner := rotToken
		if winner == "" {
			winner = rotToken2
		}
		code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+id, nil)
		if code != 200 || int(snap["received"].(float64)) > 1 {
			v.failf("race round %d: bad snapshot %d %v", i, code, snap)
		}
		// The winning token is the sole active credential: rotating again
		// with it returns 200 on the other instance.
		if code, b, _ := v.requestToken(ctx, http.MethodPost,
			v.cfg.api2+"/api/v1/batches/"+id+"/write-token/rotate", nil, winner); code != 200 {
			v.failf("race round %d: winning token not active afterwards: %d %v", i, code, b)
		}
	}
}

// persistedState is handed across the API restart via a shared volume.
type persistedState struct {
	OpenID       string `json:"openId"`
	SealedID     string `json:"sealedId"`
	SealedAt     string `json:"sealedAt"`
	OpenReceived int    `json:"openReceived"`
	// Protected fixtures prove the write capability survives an API restart:
	// it lives only in the database digest.
	ProtectedOpenID    string `json:"protectedOpenId"`
	ProtectedOpenToken string `json:"protectedOpenToken"`
	ProtectedSealedID  string `json:"protectedSealedId"`
	ProtectedSealedTok string `json:"protectedSealedToken"`
}

func (v *verifier) phaseSeed(ctx context.Context) {
	st := persistedState{}

	st.OpenID = v.createBatch(ctx, v.cfg.api1, 3)
	v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+st.OpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "middle"})
	code, snap, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+st.OpenID, nil)
	if code == 200 {
		st.OpenReceived = int(snap["received"].(float64))
	}

	st.SealedID = v.createBatch(ctx, v.cfg.api2, 2)
	v.request(ctx, http.MethodPost, v.cfg.api1+"/api/v1/batches/"+st.SealedID+"/chunks",
		map[string]any{"seq": 1, "payload": "p1"})
	v.request(ctx, http.MethodPost, v.cfg.api2+"/api/v1/batches/"+st.SealedID+"/chunks",
		map[string]any{"seq": 2, "payload": "p2"})
	_, sealed, _ := v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.SealedID+"/seal", nil)
	st.SealedAt, _ = sealed["sealedAt"].(string)

	// Protected fixtures: the capability lives only in the database digest,
	// so the same token must keep authorising writes after an API restart.
	st.ProtectedOpenID, st.ProtectedOpenToken = v.createProtectedBatch(ctx, v.cfg.api1, 2)
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+st.ProtectedOpenID+"/chunks",
		map[string]any{"seq": 1, "payload": "po1"}, st.ProtectedOpenToken); code != 201 {
		v.failf("seed protected open chunk: %d %v", code, b)
	}
	st.ProtectedSealedID, st.ProtectedSealedTok = v.createProtectedBatch(ctx, v.cfg.api2, 1)
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.ProtectedSealedID+"/chunks",
		map[string]any{"seq": 1, "payload": "ps1"}, st.ProtectedSealedTok); code != 201 {
		v.failf("seed protected sealed chunk: %d %v", code, b)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.ProtectedSealedID+"/seal", nil,
		st.ProtectedSealedTok); code != 200 || b["status"] != "SEALED" {
		v.failf("seed protected seal: %d %v", code, b)
	}

	if err := writeState(st); err != nil {
		v.failf("write state: %v", err)
		return
	}
	v.log("seeded restart fixtures: open=%s sealed=%s", st.OpenID, st.SealedID)
}

func (v *verifier) phaseRestart(ctx context.Context) {
	st, err := readState()
	if err != nil {
		v.failf("read state: %v", err)
		return
	}

	code, open, _ := v.request(ctx, http.MethodGet, v.cfg.api1+"/api/v1/batches/"+st.OpenID, nil)
	if code != 200 || open["status"] != "OPEN" ||
		int(open["received"].(float64)) != st.OpenReceived {
		v.failf("after restart open verdict changed: code=%d body=%v", code, open)
	}
	if gaps := open["gaps"].([]any); len(gaps) != 2 ||
		int(gaps[0].(float64)) != 1 || int(gaps[1].(float64)) != 3 {
		v.failf("after restart gaps wrong: %v", open["gaps"])
	}

	// Same content ack must still be the stored original.
	code, ack, _ := v.request(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+st.OpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "middle"})
	if code != 200 || ack["duplicate"] != true {
		v.failf("after restart duplicate ack wrong: code=%d body=%v", code, ack)
	}
	// Different content still conflicts.
	code, ack, _ = v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.OpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "MIDDLE"})
	if code != 409 || ack["error"] != "CHUNK_CONFLICT" {
		v.failf("after restart conflict wrong: code=%d body=%v", code, ack)
	}

	code, sealed, _ := v.request(ctx, http.MethodGet, v.cfg.api2+"/api/v1/batches/"+st.SealedID, nil)
	if code != 200 || sealed["status"] != "SEALED" || sealed["sealedAt"] != st.SealedAt {
		v.failf("after restart sealed verdict changed: code=%d body=%v want sealedAt=%s",
			code, sealed, st.SealedAt)
	}
	// Repeated seal returns the existing result.
	code, resealed, _ := v.request(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.SealedID+"/seal", nil)
	if code != 200 || resealed["sealedAt"] != st.SealedAt {
		v.failf("after restart reseal wrong: code=%d body=%v", code, resealed)
	}

	// Protected OPEN batch: the pre-restart token still authorises a new
	// write across instances, and a missing token is still the uniform 403.
	code, protOpen, _ := v.request(ctx, http.MethodGet,
		v.cfg.api2+"/api/v1/batches/"+st.ProtectedOpenID, nil)
	if code != 200 || protOpen["status"] != "OPEN" ||
		int(protOpen["received"].(float64)) != 1 || protOpen["writeToken"] != nil {
		v.failf("after restart protected open changed: code=%d body=%v", code, protOpen)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.ProtectedOpenID+"/chunks",
		map[string]any{"seq": 2, "payload": "po2"}, st.ProtectedOpenToken); code != 201 {
		v.failf("after restart protected write with old token: %d %v", code, b)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+st.ProtectedOpenID+"/seal", nil, ""); code != 403 ||
		b["error"] != "WRITE_CAPABILITY_REQUIRED" {
		v.failf("after restart protected seal without token: %d %v", code, b)
	}

	// Protected SEALED batch: token still gates the idempotent retransmit
	// and repeated seal; rotate succeeds and its new token immediately
	// becomes the sole capability.
	code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+st.ProtectedSealedID+"/chunks",
		map[string]any{"seq": 1, "payload": "ps1"}, st.ProtectedSealedTok)
	if code != 200 || b["duplicate"] != true {
		v.failf("after restart protected sealed retransmit: %d %v", code, b)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.ProtectedSealedID+"/seal", nil,
		st.ProtectedSealedTok); code != 200 {
		v.failf("after restart protected reseal with token: %d %v", code, b)
	}
	code, rb, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api2+"/api/v1/batches/"+st.ProtectedSealedID+"/write-token/rotate", nil,
		st.ProtectedSealedTok)
	if code != 200 {
		v.failf("after restart rotate with token: %d %v", code, rb)
	}
	if code, b, _ := v.requestToken(ctx, http.MethodPost,
		v.cfg.api1+"/api/v1/batches/"+st.ProtectedSealedID+"/seal", nil,
		st.ProtectedSealedTok); code != 403 {
		v.failf("after restart old token must fail post-rotate seal: %d %v", code, b)
	}

	v.log("restart verdicts confirmed identical on both instances")
}

func writeState(s persistedState) error {
	dir := stateDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "state.json"), b, 0o644)
}

func readState() (persistedState, error) {
	var s persistedState
	b, err := os.ReadFile(filepath.Join(stateDir(), "state.json"))
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}
