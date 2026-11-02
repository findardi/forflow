-- +goose Up
-- Raw HMAC key bytes; whsec_ encoding is only for display. Length follows the
-- Standard Webhooks range so a weak key cannot be stored.
ALTER TABLE endpoints ADD COLUMN secret bytea NOT NULL
    CONSTRAINT endpoints_secret_len_check
    CHECK (octet_length(secret) BETWEEN 24 AND 64);

-- +goose Down
ALTER TABLE endpoints DROP COLUMN secret;
