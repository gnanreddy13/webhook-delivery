-- Endpoint health: manual disable plus a circuit breaker.
ALTER TABLE endpoints
    ADD COLUMN disabled_at          timestamptz,
    ADD COLUMN consecutive_failures int NOT NULL DEFAULT 0,
    ADD COLUMN paused_until         timestamptz;

-- Set while a worker holds a delivery (cleared when the attempt is recorded);
-- used to count in-flight deliveries per endpoint.
ALTER TABLE deliveries ADD COLUMN lease_expires_at timestamptz;

-- Claiming now picks the oldest due row per endpoint.
CREATE INDEX deliveries_endpoint_due_idx ON deliveries (endpoint_id, next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_endpoint_leased_idx ON deliveries (endpoint_id, lease_expires_at) WHERE lease_expires_at IS NOT NULL;
