-- name: CreateMessage :one
-- No row means the key is taken. A concurrent insert with the same key waits
-- for the first transaction, then returns nothing once it commits.
insert into messages
    (payload, idempotency_key)
values
    ($1, $2)
on conflict (idempotency_key) do nothing
returning *;

-- name: GetMessageByID :one
select * from messages where id = $1;

-- name: GetMessageByIdempotencyKey :one
select * from messages where idempotency_key = $1;
