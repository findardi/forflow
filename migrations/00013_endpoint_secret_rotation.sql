-- +goose Up
-- The secret replaced by the last rotation keeps signing until it expires, so
-- every attempt in that window carries both signatures and a receiver can
-- switch keys without a single failed verification.
ALTER TABLE endpoints
    ADD COLUMN previous_secret bytea
        CONSTRAINT endpoints_previous_secret_len_check
        CHECK (octet_length(previous_secret) BETWEEN 24 AND 64),
    ADD COLUMN previous_secret_expires_at timestamptz,
    ADD CONSTRAINT endpoints_previous_secret_check
        CHECK ((previous_secret IS NULL) = (previous_secret_expires_at IS NULL));

-- +goose Down
ALTER TABLE endpoints
    DROP COLUMN previous_secret_expires_at,
    DROP COLUMN previous_secret;
