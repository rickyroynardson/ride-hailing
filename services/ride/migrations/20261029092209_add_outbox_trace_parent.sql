-- +goose Up
ALTER TABLE outbox ADD COLUMN trace_parent TEXT;

-- +goose Down
ALTER TABLE outbox DROP COLUMN IF EXISTS trace_parent;
