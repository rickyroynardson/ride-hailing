-- +goose Up
CREATE TABLE charges(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- One charge per ride: this is what makes ride's at-least-once delivery safe.
    ride_id UUID NOT NULL UNIQUE,
    rider_id UUID NOT NULL,
    driver_id UUID NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('fare', 'cancellation_fee')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'captured', 'failed')),
    decline_reason TEXT,
    provider_ref TEXT,
    -- Bumped when the rider retries a declined charge; part of the provider's idempotency key, so the retry is a new
    -- charge to the provider, while repeats after a lost response are not.
    provider_attempt INT NOT NULL DEFAULT 1,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ride_notified_at TIMESTAMPTZ,
    trace_parent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'failed') = (decline_reason IS NOT NULL)),
    CHECK ((status = 'captured') = (provider_ref IS NOT NULL))
);

CREATE INDEX idx_charges_due ON charges(next_attempt_at) WHERE status = 'pending';
CREATE INDEX idx_charges_unnotified ON charges(created_at) WHERE status <> 'pending' AND ride_notified_at IS NULL;
CREATE INDEX idx_charges_rider ON charges(rider_id);

CREATE TABLE payment_methods(
    rider_id UUID PRIMARY KEY,
    token TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS payment_methods;
DROP TABLE IF EXISTS charges;
