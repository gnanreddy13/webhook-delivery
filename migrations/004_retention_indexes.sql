-- Support the retention job: find finished deliveries and old events cheaply.
CREATE INDEX deliveries_finished_idx ON deliveries (updated_at) WHERE status <> 'pending';
CREATE INDEX events_created_idx ON events (created_at);
