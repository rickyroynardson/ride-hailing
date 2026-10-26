-- +goose Up
CREATE TABLE match_requests (
    ride_id UUID PRIMARY KEY,
    pickup_lat DOUBLE PRECISION NOT NULL,
    pickup_lng DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'matching' CHECK (status IN ('matching', 'matched', 'no_driver', 'cancelled')),
    deadline_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_match_requests_matching ON match_requests(deadline_at) WHERE status = 'matching';

-- +goose Down
DROP TABLE IF EXISTS match_requests;
