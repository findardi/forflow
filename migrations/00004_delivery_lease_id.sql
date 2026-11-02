-- +goose Up
-- Rows mid-lease have no lease_id to fence with; hand them back to the queue.
UPDATE deliveries SET status = 'pending', locked_until = NULL
WHERE status = 'processing';

ALTER TABLE deliveries ADD COLUMN lease_id uuid;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_lease_check
    CHECK ((status = 'processing') = (lease_id IS NOT NULL));

-- +goose Down
ALTER TABLE deliveries DROP CONSTRAINT deliveries_lease_check;
ALTER TABLE deliveries DROP COLUMN lease_id;
