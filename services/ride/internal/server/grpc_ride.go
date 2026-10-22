package server

import (
	"context"

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
		RiderID: callerFromContext(ctx).UserID,
		Pickup:  toLocation(req.GetPickup()),
		Dropoff: toLocation(req.GetDropoff()),
	})
	if err != nil {
		return nil, err
	}

	return &ridev1.CreateRideResponse{RideId: id}, nil
}

func toLocation(l *ridev1.Location) *ride.Location {
	if l == nil {
		return nil
	}
	return &ride.Location{Lat: l.GetLat(), Lng: l.GetLng()}
}
