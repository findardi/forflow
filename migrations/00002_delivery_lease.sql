-- +goose Up
ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'processing', 'sent', 'failed'));

ALTER TABLE deliveries ADD COLUMN locked_until timestamptz;

CREATE INDEX deliveries_processing_idx ON deliveries (locked_until)
    WHERE status = 'processing';

-- +goose Down
DROP INDEX deliveries_processing_idx;

UPDATE deliveries SET status = 'pending' WHERE status = 'processing';
ALTER TABLE deliveries DROP COLUMN locked_until;

ALTER TABLE deliveries DROP CONSTRAINT deliveries_status_check;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('pending', 'sent', 'failed'));
