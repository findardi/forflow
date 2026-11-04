-- +goose Up
-- One row per contact with an endpoint; deliveries keeps only the latest state.
-- Partitioned by day so retention drops a table instead of deleting rows.
-- created_at is the attempt start, passed from Go (injected clock), and is part
-- of the PK because Postgres can only enforce uniqueness per partition.
CREATE TABLE attempts (
    id              uuid        NOT NULL DEFAULT uuidv7(),
    delivery_id     uuid        NOT NULL REFERENCES deliveries (id),
    attempt_number  int         NOT NULL CHECK (attempt_number > 0),
    succeeded       boolean     NOT NULL,
    response_status int,
    error_kind      text,
    duration_ms     int         NOT NULL CHECK (duration_ms >= 0),
    created_at      timestamptz NOT NULL,
    PRIMARY KEY (created_at, id),
    -- Every attempt either got a response or failed in transport.
    CONSTRAINT attempts_result_check
        CHECK (response_status IS NOT NULL OR error_kind IS NOT NULL)
) PARTITION BY RANGE (created_at);

CREATE INDEX attempts_delivery_idx ON attempts (delivery_id, created_at);

-- Bounds are formatted with to_char as explicit UTC, so neither DateStyle nor
-- the session TimeZone can shift them.
-- Starts at yesterday: an attempt that began before midnight may be recorded after.
-- lock_timeout: creating or dropping a partition takes ACCESS EXCLUSIVE on the
-- parent; give up instead of queueing every insert behind a long dashboard query.
-- +goose StatementBegin
CREATE FUNCTION ensure_attempt_partitions(days_ahead int) RETURNS void
LANGUAGE plpgsql SET lock_timeout = '1s' AS $$
DECLARE
    today date := (now() AT TIME ZONE 'UTC')::date;
    day   date;
BEGIN
    FOR i IN -1..days_ahead LOOP
        day := today + i;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF attempts FOR VALUES FROM (%L) TO (%L)',
            'attempts_' || to_char(day, 'YYYYMMDD'),
            to_char(day, 'YYYY-MM-DD') || ' 00:00+00',
            to_char(day + 1, 'YYYY-MM-DD') || ' 00:00+00');
    END LOOP;
END;
$$;
-- +goose StatementEnd

-- Relies on the attempts_YYYYMMDD naming: name order equals date order.
-- +goose StatementBegin
CREATE FUNCTION drop_attempt_partitions(keep_days int) RETURNS void
LANGUAGE plpgsql SET lock_timeout = '1s' AS $$
DECLARE
    cutoff text;
    part   text;
BEGIN
    IF keep_days < 1 THEN
        RAISE EXCEPTION 'keep_days must be >= 1, got %', keep_days;
    END IF;
    cutoff := 'attempts_' || to_char((now() AT TIME ZONE 'UTC')::date - keep_days, 'YYYYMMDD');
    FOR part IN
        SELECT c.relname FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'attempts'::regclass
          AND c.relname < cutoff
    LOOP
        EXECUTE format('DROP TABLE %I', part);
    END LOOP;
END;
$$;
-- +goose StatementEnd

SELECT ensure_attempt_partitions(3);

-- +goose Down
DROP FUNCTION drop_attempt_partitions(int);
DROP FUNCTION ensure_attempt_partitions(int);
DROP TABLE attempts;
