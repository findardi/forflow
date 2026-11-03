-- +goose Up
-- ClaimDelivery already knows the endpoint; without this it walks every due
-- delivery of other endpoints to find that endpoint's oldest one.
CREATE INDEX deliveries_endpoint_due_idx ON deliveries (endpoint_id, next_attempt_at)
    WHERE status IN ('pending', 'processing');

-- +goose Down
DROP INDEX deliveries_endpoint_due_idx;
