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

	if err := s.service.UpdateDriverLocation(ctx, in); err != nil {
		return nil, err
	}

	return &locationv1.UpdateDriverLocationResponse{}, nil
}

func toLocation(l *locationv1.Location) *location.Location {
	if l == nil {
		return nil
	}
	return &location.Location{Lat: l.GetLat(), Lng: l.GetLng()}
}
