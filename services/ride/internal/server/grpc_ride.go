package server

import (
	"context"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"

	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
)

type RideServer struct {
	ridev1.UnimplementedRideServiceServer
	service *ride.RideService
}

func NewRideServer(service *ride.RideService) *RideServer {
	return &RideServer{service: service}
}

func (s *RideServer) CreateRide(ctx context.Context, req *ridev1.CreateRideRequest) (*ridev1.CreateRideResponse, error) {
	id, err := s.service.CreateRide(ctx, ride.CreateRideInput{
		RiderID: grpcserver.CallerFrom(ctx).UserID,
		Pickup:  toLocation(req.GetPickup()),
		Dropoff: toLocation(req.GetDropoff()),
	})
	if err != nil {
		return nil, err
	}

	return &ridev1.CreateRideResponse{RideId: id}, nil
}

func (s *RideServer) FilterAvailableDrivers(ctx context.Context, req *ridev1.FilterAvailableDriversRequest) (*ridev1.FilterAvailableDriversResponse, error) {
	ids, err := s.service.FilterAvailableDrivers(ctx, req.GetDriverIds())
	if err != nil {
		return nil, err
	}

	return &ridev1.FilterAvailableDriversResponse{DriverIds: ids}, nil
}

func (s *RideServer) AssignDriver(ctx context.Context, req *ridev1.AssignDriverRequest) (*ridev1.AssignDriverResponse, error) {
	if err := s.service.AssignDriver(ctx, req.GetRideId(), req.GetDriverId()); err != nil {
		return nil, err
	}

	return &ridev1.AssignDriverResponse{}, nil
}

func (s *RideServer) MarkNoDriver(ctx context.Context, req *ridev1.MarkNoDriverRequest) (*ridev1.MarkNoDriverResponse, error) {
	if err := s.service.MarkNoDriver(ctx, req.GetRideId()); err != nil {
		return nil, err
	}

	return &ridev1.MarkNoDriverResponse{}, nil
}

func (s *RideServer) CancelRide(ctx context.Context, req *ridev1.CancelRideRequest) (*ridev1.CancelRideResponse, error) {
	if err := s.service.CancelRide(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID); err != nil {
		return nil, err
	}

	return &ridev1.CancelRideResponse{}, nil
}

func toLocation(l *ridev1.Location) *ride.Location {
	if l == nil {
		return nil
	}
	return &ride.Location{Lat: l.GetLat(), Lng: l.GetLng()}
}
