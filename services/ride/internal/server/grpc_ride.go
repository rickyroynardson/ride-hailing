package server

import (
	"context"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"go.opentelemetry.io/otel/attribute"
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

	telemetry.Tag(ctx, attribute.String("ride.id", id))
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
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.AssignDriver(ctx, req.GetRideId(), req.GetDriverId()); err != nil {
		return nil, err
	}

	return &ridev1.AssignDriverResponse{}, nil
}

func (s *RideServer) MarkNoDriver(ctx context.Context, req *ridev1.MarkNoDriverRequest) (*ridev1.MarkNoDriverResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.MarkNoDriver(ctx, req.GetRideId()); err != nil {
		return nil, err
	}

	return &ridev1.MarkNoDriverResponse{}, nil
}

func (s *RideServer) CancelRide(ctx context.Context, req *ridev1.CancelRideRequest) (*ridev1.CancelRideResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.CancelRide(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID); err != nil {
		return nil, err
	}

	return &ridev1.CancelRideResponse{}, nil
}

func (s *RideServer) StartRide(ctx context.Context, req *ridev1.StartRideRequest) (*ridev1.StartRideResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.StartRide(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID); err != nil {
		return nil, err
	}

	return &ridev1.StartRideResponse{}, nil
}

func (s *RideServer) CompleteRide(ctx context.Context, req *ridev1.CompleteRideRequest) (*ridev1.CompleteRideResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.CompleteRide(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID); err != nil {
		return nil, err
	}

	return &ridev1.CompleteRideResponse{}, nil
}

func toLocation(l *ridev1.Location) *ride.Location {
	if l == nil {
		return nil
	}
	return &ride.Location{Lat: l.GetLat(), Lng: l.GetLng()}
}
