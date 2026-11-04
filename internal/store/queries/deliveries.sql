-- name: CreateDeliveriesForMessage :execrows
-- Disabled endpoints get no new deliveries; their backlog waits for re-enable.
insert into deliveries
    (message_id, endpoint_id)
select
    $1, id
from endpoints
where disabled_at is null;

-- NotifyDeliveries wakes idle workers. Postgres delivers it only when the
-- calling transaction commits, so a rolled-back ingest never wakes anyone.
-- The channel name must match the LISTEN in internal/scheduler.
-- name: NotifyDeliveries :exec
select pg_notify('forflow_deliveries', '');

-- name: GetDeliveryByID :one
select * from deliveries where id = $1;

-- name: GetDeliveriesByMessageID :many
select * from deliveries where message_id = $1;

-- Outcome queries below are fenced by lease_id: zero affected rows means the
-- lease was lost to another worker and the caller's result must be dropped.

-- name: MarkSent :execrows
update deliveries
set
    status = 'sent',
    response_status = $3,
    attempted_at = now(),
    attempt_count = attempt_count + 1,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- name: ScheduleRetry :execrows
update deliveries
set
    status = 'pending',
    response_status = $3,
    attempted_at = now(),
    attempt_count = attempt_count + 1,
    next_attempt_at = $4,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- name: MarkFailed :execrows
update deliveries
set
    status = 'failed',
    response_status = $3,
    attempted_at = now(),
    attempt_count = attempt_count + 1,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- MarkDiscarded is for permanent errors on our side: the endpoint was never
-- contacted, so the attempt budget is left untouched.
-- name: MarkDiscarded :execrows
update deliveries
set
    status = 'discarded',
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- ReleaseLease hands a claimed delivery back without spending an attempt, e.g.
-- when the worker shuts down before sending.
-- name: ReleaseLease :execrows
update deliveries
set
    status = 'pending',
    next_attempt_at = now(),
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;