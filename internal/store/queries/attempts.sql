-- InsertAttempt runs in the same transaction as the fenced outcome update;
-- the caller rolls back when that update hits zero rows, so a lost lease
-- leaves no attempt behind.
-- name: InsertAttempt :exec
insert into attempts
    (delivery_id, attempt_number, succeeded, response_status, error_kind, duration_ms, created_at)
values
    ($1, $2, $3, $4, $5, $6, $7);

-- name: ListAttemptsByDelivery :many
select * from attempts
where delivery_id = $1
order by created_at, id;

-- name: EnsureAttemptPartitions :exec
select ensure_attempt_partitions(@days_ahead::int);

-- name: DropAttemptPartitions :exec
select drop_attempt_partitions(@keep_days::int);
