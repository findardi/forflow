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

-- name: RecordDelivery :exec
update deliveries set
status = $1,
response_status = $2,
attempted_at = now(),
locked_until = null
where id = $3 and status = 'processing';