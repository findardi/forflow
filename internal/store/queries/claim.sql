-- Claiming is two statements in one transaction. Under READ COMMITTED each
-- statement takes a fresh snapshot, so counting in a second statement, after
-- the endpoint lock is held, sees every claim committed by the lock's previous
-- holder. Counting in the same statement that takes the lock can read a
-- snapshot from before that commit and overshoot the cap.
--
-- The circuit breaker narrows the cap per endpoint: closed while its failure
-- streak is under breaker_threshold (cap = max_in_flight), open while
-- paused_until is in the future (skipped), half-open after that (cap = 1, so a
-- single probe decides). Disabled endpoints are never claimed; their backlog
-- stays pending until the endpoint is enabled again.

-- name: LockClaimableEndpoint :one
-- Picks the endpoint owning the oldest due delivery and locks its row until the
-- transaction ends. The in-flight count here is only a coarse filter; it may be
-- stale, and ClaimDelivery counts again. The endpoint conditions need no second
-- check: FOR UPDATE re-evaluates them against the latest row version, and the
-- lock keeps record from changing them until this transaction ends.
select c.endpoint_id
from deliveries c
join endpoints ce on ce.id = c.endpoint_id
where c.status in ('pending', 'processing')
and c.next_attempt_at <= now()
and ce.disabled_at is null
and (ce.paused_until is null or ce.paused_until <= now())
and (
    select count(*) from deliveries f
    where f.endpoint_id = ce.id
    and f.status = 'processing'
    and f.next_attempt_at > now()
) < case
    when ce.consecutive_failures >= sqlc.arg(breaker_threshold)::int then 1
    else sqlc.arg(max_in_flight)::int
end
order by c.next_attempt_at asc
limit 1
for update of ce skip locked;

-- name: ClaimDelivery :one
-- Must run in the transaction that holds the endpoint lock from
-- LockClaimableEndpoint. Returns no row when the endpoint is already at its cap.
update deliveries d
set status = 'processing',
    lease_id = uuidv7(),
    next_attempt_at = now() + sqlc.arg(lease)::interval
from messages m, endpoints e
where d.id = (
    select c.id
    from deliveries c
    join endpoints ce on ce.id = c.endpoint_id
    where c.endpoint_id = sqlc.arg(endpoint_id)::uuid
    and c.status in ('pending', 'processing')
    and c.next_attempt_at <= now()
    and (
        select count(*) from deliveries f
        where f.endpoint_id = sqlc.arg(endpoint_id)::uuid
        and f.status = 'processing'
        and f.next_attempt_at > now()
    ) < case
        when ce.consecutive_failures >= sqlc.arg(breaker_threshold)::int then 1
        else sqlc.arg(max_in_flight)::int
    end
    order by c.next_attempt_at asc
    limit 1
    for update of c skip locked
)
and m.id = d.message_id
and e.id = d.endpoint_id
returning d.id, d.endpoint_id, e.url as endpoint, m.payload, d.lease_id, d.attempt_count, d.message_id, e.secret;
