CREATE TABLE endpoints (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    url         text NOT NULL,
    event_types text[] NOT NULL,
    secret      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE events (
    id         text PRIMARY KEY,
    type       text NOT NULL,
    payload    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id               bigserial PRIMARY KEY,
    event_id         text NOT NULL REFERENCES events(id),
    endpoint_id      uuid NOT NULL REFERENCES endpoints(id),
    status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'delivered', 'dead')),
    attempts         int NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    last_status_code int,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    delivered_at     timestamptz,
    UNIQUE (event_id, endpoint_id)
);

-- Workers scan only pending rows, oldest due first.
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_status_idx ON deliveries (status, id);
