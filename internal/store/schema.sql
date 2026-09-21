-- Batch sealing service schema.
-- Applied at API startup; idempotent so any instance can boot first.

CREATE TABLE IF NOT EXISTS batches (
    id               CHAR(32)     PRIMARY KEY,
    expected_chunks  INTEGER      NOT NULL CHECK (expected_chunks BETWEEN 1 AND 10000),
    status           TEXT         NOT NULL DEFAULT 'OPEN'
                                   CHECK (status IN ('OPEN', 'SEALED')),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sealed_at        TIMESTAMPTZ,
    write_token_digest BYTEA
);

ALTER TABLE batches
    ADD COLUMN IF NOT EXISTS write_token_digest BYTEA;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint c
          JOIN pg_namespace n ON n.oid = c.connamespace
         WHERE c.conname = 'batches_write_token_digest_len'
           AND n.nspname = current_schema()
    ) THEN
        ALTER TABLE batches
            ADD CONSTRAINT batches_write_token_digest_len
            CHECK (write_token_digest IS NULL OR octet_length(write_token_digest) = 32);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS chunks (
    batch_id    CHAR(32)     NOT NULL
        REFERENCES batches (id) ON DELETE CASCADE,
    seq         INTEGER      NOT NULL CHECK (seq >= 1),
    payload     BYTEA        NOT NULL,
    received_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, seq)
);

-- Covered by the composite primary key, but make reverse lookups explicit.
CREATE INDEX IF NOT EXISTS idx_chunks_batch ON chunks (batch_id);
