-- +goose Up
-- Lease expiry moved to next_attempt_at in 00003/00005; locked_until is
-- written but never read.
ALTER TABLE deliveries DROP COLUMN locked_until;

-- +goose Down
-- Restored empty; 00005 Down rebuilds an index on this column.
ALTER TABLE deliveries ADD COLUMN locked_until timestamptz;
