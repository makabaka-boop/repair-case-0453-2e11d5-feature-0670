-- Batch sealing service schema.
-- Applied at API startup; idempotent so any instance can boot first.

CREATE TABLE IF NOT EXISTS batches (
    id               CHAR(32)     PRIMARY KEY,
    expected_chunks  INTEGER      NOT NULL CHECK (expected_chunks BETWEEN 1 AND 10000),
    status           TEXT         NOT NULL DEFAULT 'OPEN'
                                   CHECK (status IN ('OPEN', 'SEALED')),
    -- SHA-256 digest (32 bytes) of the active write token. Rows created
    -- before write protection existed (and batches created with
    -- protectWrites omitted/false) leave this NULL and are treated as
    -- unprotected. The raw token is never stored.
    write_token_digest BYTEA,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sealed_at        TIMESTAMPTZ
);

-- Idempotent upgrade for databases initialised before write protection:
-- CREATE TABLE IF NOT EXISTS above does not add columns to an existing table.
ALTER TABLE batches ADD COLUMN IF NOT EXISTS write_token_digest BYTEA;

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
