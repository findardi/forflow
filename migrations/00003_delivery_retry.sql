-- +goose Up
ALTER TABLE deliveries
    ADD COLUMN attempt_count   int         NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'processing', 'sent', 'failed', 'discarded'));

DROP INDEX deliveries_pending_idx;
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at)
    WHERE status = 'pending';

-- +goose Down
DROP INDEX deliveries_due_idx;
CREATE INDEX deliveries_pending_idx ON deliveries (id) WHERE status = 'pending';

UPDATE deliveries SET status = 'failed' WHERE status = 'discarded';
ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'processing', 'sent', 'failed'));

ALTER TABLE deliveries
    DROP COLUMN next_attempt_at,
    DROP COLUMN attempt_count;
