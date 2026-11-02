-- +goose Up
-- Nullable: rides from before pricing have no fare, and are never charged.
ALTER TABLE rides ADD COLUMN fare_amount BIGINT CHECK (fare_amount > 0);
ALTER TABLE rides ADD COLUMN fare_currency TEXT;
ALTER TABLE rides ADD COLUMN payment_status TEXT CHECK (payment_status IN ('pending', 'paid', 'failed'));

CREATE INDEX rides_payment_failed ON rides(rider_id) WHERE payment_status = 'failed';

-- +goose Down
DROP INDEX IF EXISTS rides_payment_failed;
ALTER TABLE rides DROP COLUMN IF EXISTS payment_status;
ALTER TABLE rides DROP COLUMN IF EXISTS fare_currency;
ALTER TABLE rides DROP COLUMN IF EXISTS fare_amount;
