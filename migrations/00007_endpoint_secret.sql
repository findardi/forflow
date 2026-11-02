-- +goose Up
-- Raw HMAC key bytes; whsec_ encoding is only for display. Length follows the
-- Standard Webhooks range so a weak key cannot be stored.
-- The volatile default gives existing rows distinct random keys (two v4 UUIDs,
-- generated from a strong random source = 32 bytes), then is dropped so new
-- secrets always come from Go.
ALTER TABLE endpoints ADD COLUMN secret bytea NOT NULL
    DEFAULT uuid_send(gen_random_uuid()) || uuid_send(gen_random_uuid())
    CONSTRAINT endpoints_secret_len_check
    CHECK (octet_length(secret) BETWEEN 24 AND 64);
ALTER TABLE endpoints ALTER COLUMN secret DROP DEFAULT;

-- +goose Down
ALTER TABLE endpoints DROP COLUMN secret;
