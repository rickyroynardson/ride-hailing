-- +goose Up
-- Old rides have no key. New bookings require one; scope it to the rider so it cannot reveal another rider's trip.
ALTER TABLE rides ADD COLUMN request_id UUID;
CREATE UNIQUE INDEX rides_booking_request ON rides(rider_id, request_id) WHERE request_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS rides_booking_request;
ALTER TABLE rides DROP COLUMN request_id;
