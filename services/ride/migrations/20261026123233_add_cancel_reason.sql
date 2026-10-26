-- +goose Up
ALTER TABLE rides ADD COLUMN cancel_reason TEXT CHECK (cancel_reason IN ('rider', 'no_driver'));
ALTER TABLE rides ADD CONSTRAINT rides_cancel_reason_matches_status CHECK ((status = 'cancelled') = (cancel_reason IS NOT NULL));

-- +goose Down
ALTER TABLE rides DROP CONSTRAINT IF EXISTS rides_cancel_reason_matches_status;
ALTER TABLE rides DROP COLUMN IF EXISTS cancel_reason;