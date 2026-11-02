-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION notify_ride_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('ride_changed', NEW.id::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER rides_notify_changed AFTER UPDATE ON rides
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION notify_ride_changed();

-- +goose Down
DROP TRIGGER IF EXISTS rides_notify_changed ON rides;
DROP FUNCTION IF EXISTS notify_ride_changed();
