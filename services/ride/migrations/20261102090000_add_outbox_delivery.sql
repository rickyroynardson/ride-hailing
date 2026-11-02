-- +goose Up
ALTER TABLE outbox ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE outbox ADD COLUMN dead_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE outbox DROP COLUMN IF EXISTS dead_at;
ALTER TABLE outbox DROP COLUMN IF EXISTS next_attempt_at;
