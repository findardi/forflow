-- name: ClaimDelivery :one
update deliveries d
set status = 'processing', 
    locked_until = now() + sqlc.arg(lease)::interval, 
    lease_id = uuidv7(), 
    next_attempt_at = now() + sqlc.arg(lease)::interval
from messages m, endpoints e
where d.id = (
    select id from deliveries
    where status in ('pending', 'processing')
    and next_attempt_at <= now()
    order by next_attempt_at asc
    limit 1
    for update skip locked
)
and m.id = d.message_id
and e.id = d.endpoint_id
returning d.id, e.url as endpoint, m.payload, d.lease_id, d.attempt_count, d.message_id;
