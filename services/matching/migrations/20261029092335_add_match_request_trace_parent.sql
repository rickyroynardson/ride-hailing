-- +goose Up
ALTER TABLE match_requests ADD COLUMN trace_parent TEXT;

-- +goose Down
ALTER TABLE match_requests DROP COLUMN IF EXISTS trace_parent;
