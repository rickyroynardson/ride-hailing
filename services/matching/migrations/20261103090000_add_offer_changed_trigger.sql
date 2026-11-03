-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION notify_offer_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('offer_changed', NEW.driver_id::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER offers_notify_changed AFTER INSERT OR UPDATE OF status ON offers
    FOR EACH ROW EXECUTE FUNCTION notify_offer_changed();

-- +goose Down
DROP TRIGGER IF EXISTS offers_notify_changed ON offers;
DROP FUNCTION IF EXISTS notify_offer_changed();
