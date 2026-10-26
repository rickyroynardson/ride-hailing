-- +goose Up
-- set once ride has confirmed it knows the request ended without a driver; until then Tick keeps telling it
ALTER TABLE match_requests ADD COLUMN ride_notified_at TIMESTAMPTZ;

CREATE INDEX idx_match_requests_unnotified ON match_requests(updated_at) WHERE status = 'no_driver' AND ride_notified_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_match_requests_unnotified;
ALTER TABLE match_requests DROP COLUMN IF EXISTS ride_notified_at;
