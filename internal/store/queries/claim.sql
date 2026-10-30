-- name: ClaimDelivery :one
update deliveries d
set status = 'processing', locked_until = now() + sqlc.arg(lease)::interval
from messages m, endpoints e
where d.id = (
    select id from deliveries
    where status = 'pending' or (status = 'processing' and locked_until < now())
    order by id asc
    limit 1
    for update skip locked
)
and m.id = d.message_id
and e.id = d.endpoint_id
returning d.id, e.url as endpoint, m.payload;
