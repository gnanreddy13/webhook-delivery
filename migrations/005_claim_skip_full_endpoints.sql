-- Fix: claim_deliveries picked the endpoint with the oldest due delivery
-- even when that endpoint was already at ENDPOINT_MAX_INFLIGHT. The second
-- statement then (correctly) claimed nothing, so the worker got an empty
-- claim and backed off while other endpoints had work waiting. A slow
-- endpoint at its cap therefore slowed down every other endpoint (seen in
-- CI: 40 healthy deliveries took 1.1s instead of milliseconds).
--
-- Same function as 003, plus a cap pre-filter in the first statement.
CREATE OR REPLACE FUNCTION claim_deliveries(p_limit int, p_lease_ms bigint, p_max_inflight int)
RETURNS TABLE (
    delivery_id      bigint,
    attempts         int,
    lease_until      timestamptz,
    event_id         text,
    event_type       text,
    payload          jsonb,
    event_created_at timestamptz,
    endpoint_id      uuid,
    url              text,
    secret           text
)
LANGUAGE plpgsql AS $$
#variable_conflict use_column
DECLARE
    ep_ids uuid[];
BEGIN
    SELECT array_agg(s.id) INTO ep_ids FROM (
        SELECT ep.id
        FROM endpoints ep
        WHERE ep.disabled_at IS NULL
          AND (ep.paused_until IS NULL OR ep.paused_until <= now())
          -- Skip endpoints already at their cap, so the claim moves on to
          -- other endpoints instead of picking a full one and returning
          -- nothing. This count may be slightly stale; the second statement
          -- below still enforces the cap exactly.
          AND (SELECT count(*) FROM deliveries x
               WHERE x.endpoint_id = ep.id AND x.lease_expires_at > clock_timestamp()) < p_max_inflight
          AND EXISTS (SELECT 1 FROM deliveries d
                      WHERE d.endpoint_id = ep.id AND d.status = 'pending' AND d.next_attempt_at <= now())
        ORDER BY (SELECT min(d.next_attempt_at) FROM deliveries d
                  WHERE d.endpoint_id = ep.id AND d.status = 'pending')
        LIMIT p_limit
        FOR NO KEY UPDATE OF ep SKIP LOCKED
    ) s;

    IF ep_ids IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH due AS (
        SELECT d.id
        FROM unnest(ep_ids) AS e(id)
        CROSS JOIN LATERAL (
            SELECT dd.id FROM deliveries dd
            WHERE dd.endpoint_id = e.id AND dd.status = 'pending' AND dd.next_attempt_at <= now()
            ORDER BY dd.next_attempt_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        ) d
        WHERE (SELECT count(*) FROM deliveries x
               WHERE x.endpoint_id = e.id AND x.lease_expires_at > clock_timestamp()) < p_max_inflight
    )
    UPDATE deliveries d
    SET next_attempt_at  = clock_timestamp() + p_lease_ms * interval '1 millisecond',
        lease_expires_at = clock_timestamp() + p_lease_ms * interval '1 millisecond',
        updated_at       = clock_timestamp()
    FROM due, events ev, endpoints ep
    WHERE d.id = due.id AND ev.id = d.event_id AND ep.id = d.endpoint_id
    RETURNING d.id, d.attempts, d.next_attempt_at, ev.id, ev.type, ev.payload, ev.created_at, ep.id, ep.url, ep.secret;
END;
$$;
