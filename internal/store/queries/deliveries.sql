-- name: CreateDeliveriesForMessage :execrows
insert into deliveries
    (message_id, endpoint_id)
select
    $1, id
from endpoints;

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
    locked_until = null,
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
    locked_until = null,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- name: MarkFailed :execrows
update deliveries
set
    status = 'failed',
    response_status = $3,
    attempted_at = now(),
    attempt_count = attempt_count + 1,
    locked_until = null,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- MarkDiscarded is for permanent errors on our side: the endpoint was never
-- contacted, so the attempt budget is left untouched.
-- name: MarkDiscarded :execrows
update deliveries
set
    status = 'discarded',
    locked_until = null,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;

-- ReleaseLease hands a claimed delivery back without spending an attempt, e.g.
-- when the worker shuts down before sending.
-- name: ReleaseLease :execrows
update deliveries
set
    status = 'pending',
    next_attempt_at = now(),
    locked_until = null,
    lease_id = null
where id = $1 and status = 'processing' and lease_id = $2;