package server

import (
	"context"

	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
)

type MatchingServer struct {
	matchingv1.UnimplementedMatchingServiceServer
	service *matching.MatchingService
}

func NewMatchingServer(service *matching.MatchingService) *MatchingServer {
	return &MatchingServer{service: service}
}

func (s *MatchingServer) StartMatching(ctx context.Context, req *matchingv1.StartMatchingRequest) (*matchingv1.StartMatchingResponse, error) {
	in := matching.StartInput{RideID: req.GetRideId()}
	if p := req.GetPickup(); p != nil {
		in.Pickup = &matching.Location{Lat: p.GetLat(), Lng: p.GetLng()}
	}

	offer, err := s.service.StartMatching(ctx, in)
	if err != nil || offer == nil {
		return &matchingv1.StartMatchingResponse{}, err
	}

	return &matchingv1.StartMatchingResponse{OfferId: offer.ID, DriverId: offer.DriverID}, nil
}
