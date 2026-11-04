-- +goose Up
-- Endpoint health for the circuit breaker and auto-disable. Breaker state is
-- derived, not stored: closed while consecutive_failures is under the
-- threshold, open while paused_until is in the future, half-open after that.
-- The threshold lives in config, so a stored state could disagree with it.
ALTER TABLE endpoints
    ADD COLUMN consecutive_failures int NOT NULL DEFAULT 0,
    ADD COLUMN first_failure_at timestamptz,
    ADD COLUMN paused_until timestamptz,
    ADD COLUMN disabled_at timestamptz;

-- A failure streak always has a start; a clean endpoint has none.
ALTER TABLE endpoints ADD CONSTRAINT endpoints_failure_streak_check
    CHECK (consecutive_failures >= 0
       AND (consecutive_failures = 0) = (first_failure_at IS NULL));

-- +goose Down
ALTER TABLE endpoints DROP CONSTRAINT endpoints_failure_streak_check;
ALTER TABLE endpoints
    DROP COLUMN disabled_at,
    DROP COLUMN paused_until,
    DROP COLUMN first_failure_at,
    DROP COLUMN consecutive_failures;
