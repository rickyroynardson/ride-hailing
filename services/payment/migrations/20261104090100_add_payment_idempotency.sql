-- +goose Up
-- Snapshot the card once a provider attempt starts; changing the payment method must not change a request whose
-- outcome is still unknown. A new, explicitly requested attempt resets this to NULL.
ALTER TABLE charges ADD COLUMN provider_token TEXT;

CREATE TABLE charge_retry_requests (
    charge_id UUID NOT NULL REFERENCES charges(id),
    request_id UUID NOT NULL,
    PRIMARY KEY (charge_id, request_id)
);

-- Development provider state, deliberately separate from the application ledger: this models a provider that
-- remembers operations after a lost reply or payment-service restart. No real money or card numbers live here.
CREATE TABLE fake_provider_operations (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    request JSONB NOT NULL,
    result JSONB NOT NULL,
    PRIMARY KEY (kind, key)
);
CREATE TABLE fake_provider_charges (
    reference TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    refunded BIGINT NOT NULL DEFAULT 0 CHECK (refunded >= 0 AND refunded <= amount)
);

-- +goose Down
DROP TABLE fake_provider_charges;
DROP TABLE fake_provider_operations;
DROP TABLE charge_retry_requests;
ALTER TABLE charges DROP COLUMN provider_token;
