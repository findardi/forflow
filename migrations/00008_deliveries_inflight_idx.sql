-- +goose Up
-- The per-endpoint cap counts an endpoint's in-flight rows (processing with an
-- unexpired lease) on every claim; without this the count scans all deliveries.
CREATE INDEX deliveries_inflight_idx ON deliveries (endpoint_id, next_attempt_at)
    WHERE status = 'processing';

-- +goose Down
DROP INDEX deliveries_inflight_idx;
