package server

import (
	"context"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"

	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// The driver RPCs need the driver claim: only approved drivers get offers.

func (s *MatchingServer) GetOffer(ctx context.Context, _ *matchingv1.GetOfferRequest) (*matchingv1.GetOfferResponse, error) {
	caller := grpcserver.CallerFrom(ctx)
	if !caller.Driver {
		return nil, matching.ErrNotDriver
	}
	o, err := s.service.GetOffer(ctx, caller.UserID)
	if err != nil || o == nil {
		return &matchingv1.GetOfferResponse{}, err
	}

	return &matchingv1.GetOfferResponse{Offer: &matchingv1.Offer{
		OfferId:   o.ID,
		RideId:    o.RideID,
		Pickup:    &matchingv1.Location{Lat: o.Pickup.Lat, Lng: o.Pickup.Lng},
		ExpiresAt: timestamppb.New(o.ExpiresAt),
	}}, nil
}

func (s *MatchingServer) AcceptOffer(ctx context.Context, req *matchingv1.AcceptOfferRequest) (*matchingv1.AcceptOfferResponse, error) {
	caller := grpcserver.CallerFrom(ctx)
	if !caller.Driver {
		return nil, matching.ErrNotDriver
	}
	rideID, err := s.service.AcceptOffer(ctx, req.GetOfferId(), caller.UserID)
	if err != nil {
		return nil, err
	}

	return &matchingv1.AcceptOfferResponse{RideId: rideID}, nil
}

func (s *MatchingServer) DeclineOffer(ctx context.Context, req *matchingv1.DeclineOfferRequest) (*matchingv1.DeclineOfferResponse, error) {
	caller := grpcserver.CallerFrom(ctx)
	if !caller.Driver {
		return nil, matching.ErrNotDriver
	}
	if err := s.service.DeclineOffer(ctx, req.GetOfferId(), caller.UserID); err != nil {
		return nil, err
	}

	return &matchingv1.DeclineOfferResponse{}, nil
}

func (s *MatchingServer) CancelMatching(ctx context.Context, req *matchingv1.CancelMatchingRequest) (*matchingv1.CancelMatchingResponse, error) {
	if err := s.service.CancelMatching(ctx, req.GetRideId()); err != nil {
		return nil, err
	}

	return &matchingv1.CancelMatchingResponse{}, nil
}
