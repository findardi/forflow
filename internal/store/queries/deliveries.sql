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
