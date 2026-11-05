-- name: CreateEndpoint :one
insert into endpoints
    (url, secret)
values
    ($1, $2)
returning *;

-- name: GetEndpointByID :one
select * from endpoints where id = $1;

-- Health queries run in the record transaction, after the lease-fenced outcome
-- update succeeded, so an attempt that lost its lease never touches the endpoint.

-- name: RecordEndpointSuccess :exec
-- Any 2xx closes the breaker. Skips the write when there is no streak, so the
-- common path adds no row update.
update endpoints
set consecutive_failures = 0,
    first_failure_at = null,
    paused_until = null
where id = $1 and consecutive_failures > 0;

-- name: RecordEndpointFailure :one
-- Extends the failure streak. From the threshold on, the breaker opens for a
-- cooldown that doubles with each further failure, capped at max_cooldown.
-- Failures that land while the breaker is already open (attempts claimed
-- before it opened) keep the current pause instead of extending it. A streak
-- older than disable_after disables the endpoint. SET expressions read the row
-- as it was before this update. disabled_now is true only for the call that
-- disabled it, so the caller logs the transition once.
update endpoints
set consecutive_failures = consecutive_failures + 1,
    first_failure_at = coalesce(first_failure_at, now()),
    paused_until = case
        when paused_until > now() then paused_until
        when consecutive_failures + 1 >= sqlc.arg(threshold)::int then
            now() + least(
                sqlc.arg(base_cooldown)::interval
                    * power(2, least(consecutive_failures + 1 - sqlc.arg(threshold)::int, 20)),
                sqlc.arg(max_cooldown)::interval)
        else paused_until
    end,
    disabled_at = case
        when disabled_at is null
         and now() - first_failure_at >= sqlc.arg(disable_after)::interval then now()
        else disabled_at
    end
where id = sqlc.arg(id)
returning coalesce(disabled_at = now(), false)::bool as disabled_now;

-- name: EnableEndpoint :execrows
-- Clears the disable and the whole failure streak: a kept first_failure_at
-- would disable the endpoint again on its next failure. The held backlog is
-- already due, so it flows again under the normal per-endpoint cap.
update endpoints
set disabled_at = null,
    consecutive_failures = 0,
    first_failure_at = null,
    paused_until = null
where id = $1;

-- name: RotateEndpointSecret :execrows
-- The replaced secret stays valid for 24 hours, Svix's default grace period.
-- Rotating again inside that window drops the oldest secret, so a receiver
-- still on it starts failing verification.
update endpoints
set previous_secret = secret,
    previous_secret_expires_at = now() + interval '24 hours',
    secret = sqlc.arg(secret)
where id = sqlc.arg(id);
