-- +goose Up
-- Producer-supplied Idempotency-Key. A retried POST /messages with the same key
-- gets the first message back instead of a new one, so webhook-id stays the
-- receiver's dedup key end to end. NULLs are distinct, so the key is optional.
-- Keys never expire yet; scope them per tenant once tenants exist.
ALTER TABLE messages ADD COLUMN idempotency_key text UNIQUE;

-- +goose Down
ALTER TABLE messages DROP COLUMN idempotency_key;
