-- +goose Up
CREATE TABLE IF NOT EXISTS rides(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rider_id UUID NOT NULL,
    driver_id UUID,
    status TEXT NOT NULL DEFAULT 'requested' CHECK (status IN ('requested', 'accepted', 'in_progress', 'completed', 'cancelled')),
    pickup_lat DOUBLE PRECISION NOT NULL CHECK (pickup_lat BETWEEN -90 AND 90),
    pickup_lng DOUBLE PRECISION NOT NULL CHECK (pickup_lng BETWEEN -180 AND 180),
    dropoff_lat DOUBLE PRECISION NOT NULL CHECK (dropoff_lat BETWEEN -90 AND 90),
    dropoff_lng DOUBLE PRECISION NOT NULL CHECK (dropoff_lng BETWEEN -180 AND 180),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX rides_one_active_per_rider ON rides(rider_id) WHERE status IN ('requested', 'accepted', 'in_progress');

CREATE UNIQUE INDEX rides_one_active_per_driver ON rides(driver_id) WHERE status IN ('accepted', 'in_progress');

-- +goose Down
DROP TABLE IF EXISTS rides;
