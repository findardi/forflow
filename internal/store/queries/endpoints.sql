-- name: CreateEndpoint :one
insert into endpoints
    (url)
values
    ($1)
returning *;

-- name: GetEndpointByID :one
select * from endpoints where id = $1;
