package store_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"batchseal/internal/store"
)

// TestLegacySchemaUpgrade is driven against a schema prepared without the
// write_token_digest column (schema name in LEGACY_SCHEMA env). It proves
// startup migration adds the column and that pre-existing rows (NULL digest)
// are treated as unprotected.
func TestLegacySchemaUpgrade(t *testing.T) {
	schema := os.Getenv("LEGACY_SCHEMA")
	if schema == "" {
		t.Skip("LEGACY_SCHEMA not set")
	}
	ctx := context.Background()
	s, err := store.New(ctx, schemaURL(schema))
	if err != nil {
		t.Fatalf("migrate/open old schema: %v", err)
	}
	defer s.Close()

	const old = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	// Old row is readable and unaffected.
	snap, err := s.Snapshot(ctx, old)
	if err != nil {
		t.Fatalf("read legacy row after upgrade: %v", err)
	}
	if snap.Status != store.StatusOpen || snap.Received != 1 {
		t.Fatalf("legacy row state wrong: %+v", snap)
	}
	// Old row accepts writes with no token and ignores a junk header.
	if _, err := s.SubmitChunk(ctx, old, 1, []byte("legacy"), ""); err != nil {
		t.Fatalf("legacy row identical retransmit no token: %v", err)
	}
	if _, err := s.SubmitChunk(ctx, old, 1, []byte("legacy"), "junk"); err != nil {
		t.Fatalf("legacy row must ignore token header: %v", err)
	}
	// Old row seals without a token.
	if _, err := s.SealBatch(ctx, old, ""); err != nil {
		t.Fatalf("legacy row seal no token: %v", err)
	}
	// Rotation on an old row is BATCH_NOT_PROTECTED.
	if _, err := s.RotateWriteToken(ctx, old, ""); !errors.Is(err, store.ErrNotProtected) {
		t.Fatalf("legacy row rotate: want ErrNotProtected, got %v", err)
	}
	// New protected batches still work on the upgraded schema.
	b, err := s.CreateBatch(ctx, 1, true)
	if err != nil {
		t.Fatalf("create protected on upgraded schema: %v", err)
	}
	if len(b.WriteToken) != 43 {
		t.Fatalf("token not issued on upgraded schema")
	}
	if _, err := s.SubmitChunk(ctx, b.ID, 1, []byte("x"), ""); !errors.Is(err, store.ErrWriteCapabilityRequired) {
		t.Fatalf("new protected row must require token: %v", err)
	}
}
