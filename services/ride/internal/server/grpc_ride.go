package server

import (
	"context"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// WatchRide streams the ride's state; see ride.RideService.WatchRide. Each message carries the whole ride, so a client
// needs only the latest one, and one that reconnects starts from the current state.
func (s *RideServer) WatchRide(req *ridev1.WatchRideRequest, stream grpc.ServerStreamingServer[ridev1.WatchRideResponse]) error {
	ctx := stream.Context()
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	return s.service.WatchRide(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID, func(r ride.Ride) error {
		return stream.Send(&ridev1.WatchRideResponse{Ride: toRideProto(r)})
	})
}

var toStatusProto = map[string]ridev1.RideStatus{
	"requested":   ridev1.RideStatus_RIDE_STATUS_REQUESTED,
	"accepted":    ridev1.RideStatus_RIDE_STATUS_ACCEPTED,
	"in_progress": ridev1.RideStatus_RIDE_STATUS_IN_PROGRESS,
	"completed":   ridev1.RideStatus_RIDE_STATUS_COMPLETED,
	"cancelled":   ridev1.RideStatus_RIDE_STATUS_CANCELLED,
}

var toCancelReasonProto = map[string]ridev1.CancelReason{
	"rider":     ridev1.CancelReason_CANCEL_REASON_RIDER,
	"no_driver": ridev1.CancelReason_CANCEL_REASON_NO_DRIVER,
}

func toRideProto(r ride.Ride) *ridev1.Ride {
	res := &ridev1.Ride{
		RideId:       r.ID,
		Status:       toStatusProto[r.Status],
		CancelReason: toCancelReasonProto[r.CancelReason],
		Pickup:       &ridev1.Location{Lat: r.Pickup.Lat, Lng: r.Pickup.Lng},
		Dropoff:      &ridev1.Location{Lat: r.Dropoff.Lat, Lng: r.Dropoff.Lng},
		DriverId:     r.DriverID,
		RequestedAt:  timestamppb.New(r.RequestedAt),
		AcceptedAt:   toTimestamp(r.AcceptedAt),
		StartedAt:    toTimestamp(r.StartedAt),
		CompletedAt:  toTimestamp(r.CompletedAt),
	}
	if l := r.DriverLocation; l != nil {
		res.DriverLocation = &ridev1.Location{Lat: l.Lat, Lng: l.Lng}
	}
	return res
}

// toTimestamp keeps a step that hasn't happened unset; timestamppb.New would turn it into 1970 or panic.
func toTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func toLocation(l *ridev1.Location) *ride.Location {
	if l == nil {
		return nil
	}
	return &ride.Location{Lat: l.GetLat(), Lng: l.GetLng()}
}
