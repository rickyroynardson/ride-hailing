-- +goose Up
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unsent ON outbox(id) WHERE sent_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS outbox;