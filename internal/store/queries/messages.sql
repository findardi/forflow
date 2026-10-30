-- name: CreateMessage :one
insert into messages
    (payload)
values
    ($1)
returning *;

-- name: GetMessageByID :one
select * from messages where id = $1;
