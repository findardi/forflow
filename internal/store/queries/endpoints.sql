-- name: CreateEndpoint :one
insert into endpoints
    (url, secret)
values
    ($1, $2)
returning *;

-- name: GetEndpointByID :one
select * from endpoints where id = $1;
