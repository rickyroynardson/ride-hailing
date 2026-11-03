-- +goose Up
CREATE TABLE refunds(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    charge_id UUID NOT NULL REFERENCES charges(id),
    amount BIGINT NOT NULL CHECK (amount > 0),
    reason TEXT NOT NULL,
    -- The caller's key: a repeated request finds this row instead of refunding again.
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'succeeded', 'failed')),
    failure_reason TEXT,
    provider_ref TEXT,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    trace_parent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'failed') = (failure_reason IS NOT NULL)),
    CHECK ((status = 'succeeded') = (provider_ref IS NOT NULL))
);

CREATE INDEX idx_refunds_due ON refunds(next_attempt_at) WHERE status = 'pending';
CREATE INDEX idx_refunds_charge ON refunds(charge_id);

-- A charge now posts more than once: its capture (no refund_id), and once per refund. Each posting balances by itself,
-- and may touch an account once.
ALTER TABLE ledger_entries ADD COLUMN refund_id UUID REFERENCES refunds(id);
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_entries_charge_id_account_key;
CREATE UNIQUE INDEX ledger_one_entry_per_posting ON ledger_entries(charge_id, refund_id, account) NULLS NOT DISTINCT;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_balanced() RETURNS trigger AS $$
BEGIN
    IF (SELECT sum(amount) FROM ledger_entries
        WHERE charge_id = NEW.charge_id AND refund_id IS NOT DISTINCT FROM NEW.refund_id) <> 0 THEN
        RAISE EXCEPTION 'ledger entries for charge % do not balance', NEW.charge_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_balanced() RETURNS trigger AS $$
BEGIN
    IF (SELECT sum(amount) FROM ledger_entries WHERE charge_id = NEW.charge_id) <> 0 THEN
        RAISE EXCEPTION 'ledger entries for charge % do not balance', NEW.charge_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- Going back discards refund postings, the one time the ledger's immutability has to step aside.
ALTER TABLE ledger_entries DISABLE TRIGGER ledger_no_change;
DELETE FROM ledger_entries WHERE refund_id IS NOT NULL;
ALTER TABLE ledger_entries ENABLE TRIGGER ledger_no_change;

DROP INDEX IF EXISTS ledger_one_entry_per_posting;
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_entries_charge_id_account_key UNIQUE (charge_id, account);
ALTER TABLE ledger_entries DROP COLUMN IF EXISTS refund_id;
DROP TABLE IF EXISTS refunds;
