package server

import (
	"context"

	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/location"
)

type LocationServer struct {
	locationv1.UnimplementedLocationServiceServer
	service *location.LocationService
}

func NewLocationServer(service *location.LocationService) *LocationServer {
	return &LocationServer{service: service}
}

func (s *LocationServer) UpdateDriverLocation(ctx context.Context, req *locationv1.UpdateDriverLocationRequest) (*locationv1.UpdateDriverLocationResponse, error) {
	caller := callerFromContext(ctx)
	if !caller.Driver {
		return nil, location.ErrNotDriver
	}

	in := location.UpdateLocationInput{
		DriverID:       caller.UserID,
		Location:       toLocation(req.GetLocation()),
		AccuracyMeters: req.GetAccuracyMeters(),
	}
	// AsTime on a nil timestamp gives 1970, which would read as a very old reading instead of a missing one.
	if req.GetRecordedAt() != nil {
		in.RecordedAt = req.GetRecordedAt().AsTime()
	}

	outcome, err := s.service.UpdateDriverLocation(ctx, in)
	if err != nil {
		return nil, err
	}

	return &locationv1.UpdateDriverLocationResponse{Outcome: toOutcomeProto[outcome]}, nil
}

var toOutcomeProto = map[location.Outcome]locationv1.UpdateOutcome{
	location.OutcomeStored:            locationv1.UpdateOutcome_UPDATE_OUTCOME_STORED,
	location.OutcomeIgnoredInaccurate: locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_INACCURATE,
	location.OutcomeIgnoredTooOld:     locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_TOO_OLD,
	location.OutcomeIgnoredOutOfOrder: locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_OUT_OF_ORDER,
}

// GoOffline needs no driver claim: a driver whose approval was just revoked must still be able to leave, and for
// anyone else it is a no-op.
func (s *LocationServer) GoOffline(ctx context.Context, _ *locationv1.GoOfflineRequest) (*locationv1.GoOfflineResponse, error) {
	if err := s.service.GoOffline(ctx, callerFromContext(ctx).UserID); err != nil {
		return nil, err
	}

	return &locationv1.GoOfflineResponse{}, nil
}

func toLocation(l *locationv1.Location) *location.Location {
	if l == nil {
		return nil
	}
	return &location.Location{Lat: l.GetLat(), Lng: l.GetLng()}
}

// ListNearbyDrivers is open to any authenticated caller: the ride service calls it for matching with the rider's
// token, and rider apps can show nearby cars.
func (s *LocationServer) ListNearbyDrivers(ctx context.Context, req *locationv1.ListNearbyDriversRequest) (*locationv1.ListNearbyDriversResponse, error) {
	drivers, err := s.service.ListNearbyDrivers(ctx, location.ListNearbyInput{
		Location:     toLocation(req.GetLocation()),
		RadiusMeters: req.GetRadiusMeters(),
		Limit:        int(req.GetLimit()),
	})
	if err != nil {
		return nil, err
	}

	res := &locationv1.ListNearbyDriversResponse{Drivers: make([]*locationv1.NearbyDriver, len(drivers))}
	for i, d := range drivers {
		res.Drivers[i] = &locationv1.NearbyDriver{
			DriverId:       d.DriverID,
			Location:       &locationv1.Location{Lat: d.Location.Lat, Lng: d.Location.Lng},
			DistanceMeters: d.DistanceMeters,
		}
	}
	return res, nil
}
