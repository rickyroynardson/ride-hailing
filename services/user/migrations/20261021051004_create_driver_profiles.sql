-- +goose Up
CREATE TABLE IF NOT EXISTS driver_profiles(
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    license_number TEXT NOT NULL UNIQUE,
    vehicle_plate TEXT NOT NULL UNIQUE,
    vehicle_model TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'suspended')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS driver_profiles;
