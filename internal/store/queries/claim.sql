-- name: ClaimDelivery :one
update deliveries d
set status = 'processing',
    lease_id = uuidv7(),
    next_attempt_at = now() + sqlc.arg(lease)::interval
from messages m, endpoints e
where d.id = (
    select c.id
    from deliveries c
    join endpoints ce on ce.id = c.endpoint_id
    where c.status in ('pending', 'processing')
    and c.next_attempt_at <= now()
    and (
        select count(*) from deliveries f
        where f.endpoint_id = ce.id
        and f.status = 'processing'
        and f.next_attempt_at > now()
    ) < sqlc.arg(max_in_flight)::int
    order by c.next_attempt_at asc
    limit 1
    for update of c, ce skip locked
)
and m.id = d.message_id
and e.id = d.endpoint_id
returning d.id, e.url as endpoint, m.payload, d.lease_id, d.attempt_count, d.message_id, e.secret;
