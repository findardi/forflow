-- +goose Up
CREATE TABLE endpoints (
    id         uuid PRIMARY KEY DEFAULT uuidv7(),
    url        text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE messages (
    id         uuid PRIMARY KEY DEFAULT uuidv7(),
    payload    bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id              uuid PRIMARY KEY DEFAULT uuidv7(),
    message_id      uuid NOT NULL REFERENCES messages (id),
    endpoint_id     uuid NOT NULL REFERENCES endpoints (id),
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'sent', 'failed')),
    response_status int,
    attempted_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (message_id, endpoint_id)
);

CREATE INDEX deliveries_pending_idx ON deliveries (id) WHERE status = 'pending';

-- +goose Down
DROP TABLE deliveries;
DROP TABLE messages;
DROP TABLE endpoints;
