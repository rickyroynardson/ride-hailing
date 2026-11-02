-- +goose Up
-- Double-entry: every charge posts entries that sum to zero, debits positive and credits negative. A mistake is fixed
-- with new entries, never by changing old ones, so the rows are immutable and the schema enforces both rules.
CREATE TABLE ledger_entries(
    id BIGSERIAL PRIMARY KEY,
    charge_id UUID NOT NULL REFERENCES charges(id),
    account TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount <> 0),
    currency TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A charge can post to an account once, so a capture repeated by a second worker fails instead of paying twice.
    UNIQUE (charge_id, account)
);

CREATE INDEX idx_ledger_account ON ledger_entries(account);

-- +goose StatementBegin
CREATE FUNCTION ledger_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger entries are immutable';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER ledger_no_change BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_immutable();

-- Deferred to commit, so a transaction can post its entries one at a time.
-- +goose StatementBegin
CREATE FUNCTION ledger_balanced() RETURNS trigger AS $$
BEGIN
    IF (SELECT sum(amount) FROM ledger_entries WHERE charge_id = NEW.charge_id) <> 0 THEN
        RAISE EXCEPTION 'ledger entries for charge % do not balance', NEW.charge_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER ledger_must_balance AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_balanced();

-- +goose Down
DROP TRIGGER IF EXISTS ledger_must_balance ON ledger_entries;
DROP FUNCTION IF EXISTS ledger_balanced();
DROP TRIGGER IF EXISTS ledger_no_change ON ledger_entries;
DROP FUNCTION IF EXISTS ledger_immutable();
DROP TABLE IF EXISTS ledger_entries;
