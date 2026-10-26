-- +goose Up
CREATE TABLE offers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ride_id UUID NOT NULL,
    driver_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'declined', 'expired', 'cancelled')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(ride_id, driver_id)
);

-- one pending offer per ride, and per driver
CREATE UNIQUE INDEX idx_offers_one_pending_per_ride ON offers(ride_id) WHERE status = 'pending';
CREATE UNIQUE INDEX idx_offers_one_pending_per_driver ON offers(driver_id) WHERE status = 'pending';

-- +goose Down
DROP TABLE IF EXISTS offers;
