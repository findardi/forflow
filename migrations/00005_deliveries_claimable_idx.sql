-- +goose Up
-- Claim now selects both states by next_attempt_at alone (an expired lease is
-- just a due row), so one partial index replaces the two per-state ones.
DROP INDEX deliveries_due_idx;
DROP INDEX deliveries_processing_idx;
CREATE INDEX deliveries_claimable_idx ON deliveries (next_attempt_at)
    WHERE status IN ('pending', 'processing');

-- +goose Down
DROP INDEX deliveries_claimable_idx;
CREATE INDEX deliveries_processing_idx ON deliveries (locked_until)
    WHERE status = 'processing';
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at)
    WHERE status = 'pending';
