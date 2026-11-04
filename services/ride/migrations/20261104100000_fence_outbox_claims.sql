-- +goose Up
-- A lease may expire while its worker is paused. Every claim advances this fencing token; a late worker may still
-- send (receivers must stay idempotent), but cannot checkpoint or reschedule a newer worker's delivery.
ALTER TABLE outbox ADD COLUMN claim_version BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE outbox DROP COLUMN claim_version;
