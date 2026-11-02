package server

import (
	"context"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/payment"
	"go.opentelemetry.io/otel/attribute"
)

type PaymentServer struct {
	paymentv1.UnimplementedPaymentServiceServer
	service *payment.PaymentService
}

func NewPaymentServer(service *payment.PaymentService) *PaymentServer {
	return &PaymentServer{service: service}
}

var fromKindProto = map[paymentv1.ChargeKind]string{
	paymentv1.ChargeKind_CHARGE_KIND_FARE:             payment.KindFare,
	paymentv1.ChargeKind_CHARGE_KIND_CANCELLATION_FEE: payment.KindCancellationFee,
}

var toKindProto = map[string]paymentv1.ChargeKind{
	payment.KindFare:            paymentv1.ChargeKind_CHARGE_KIND_FARE,
	payment.KindCancellationFee: paymentv1.ChargeKind_CHARGE_KIND_CANCELLATION_FEE,
}

var toStatusProto = map[string]paymentv1.ChargeStatus{
	payment.StatusPending:  paymentv1.ChargeStatus_CHARGE_STATUS_PENDING,
	payment.StatusCaptured: paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED,
	payment.StatusFailed:   paymentv1.ChargeStatus_CHARGE_STATUS_FAILED,
}

func (s *PaymentServer) ChargeRide(ctx context.Context, req *paymentv1.ChargeRideRequest) (*paymentv1.ChargeRideResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.ChargeRide(ctx, payment.ChargeInput{
		RideID:   req.GetRideId(),
		RiderID:  req.GetRiderId(),
		DriverID: req.GetDriverId(),
		Kind:     fromKindProto[req.GetKind()],
		Amount:   payment.Money{Amount: req.GetAmount().GetAmount(), Currency: req.GetAmount().GetCurrency()},
	}); err != nil {
		return nil, err
	}

	return &paymentv1.ChargeRideResponse{}, nil
}

func (s *PaymentServer) GetCharge(ctx context.Context, req *paymentv1.GetChargeRequest) (*paymentv1.GetChargeResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	c, err := s.service.GetCharge(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID)
	if err != nil {
		return nil, err
	}

	return &paymentv1.GetChargeResponse{Charge: &paymentv1.Charge{
		ChargeId:      c.ID,
		RideId:        c.RideID,
		Kind:          toKindProto[c.Kind],
		Status:        toStatusProto[c.Status],
		Amount:        toMoneyProto(c.Amount),
		DeclineReason: c.DeclineReason,
	}}, nil
}

func (s *PaymentServer) RetryCharge(ctx context.Context, req *paymentv1.RetryChargeRequest) (*paymentv1.RetryChargeResponse, error) {
	telemetry.Tag(ctx, attribute.String("ride.id", req.GetRideId()))
	if err := s.service.RetryCharge(ctx, req.GetRideId(), grpcserver.CallerFrom(ctx).UserID); err != nil {
		return nil, err
	}

	return &paymentv1.RetryChargeResponse{}, nil
}

func (s *PaymentServer) SetPaymentMethod(ctx context.Context, req *paymentv1.SetPaymentMethodRequest) (*paymentv1.SetPaymentMethodResponse, error) {
	if err := s.service.SetPaymentMethod(ctx, grpcserver.CallerFrom(ctx).UserID, req.GetToken()); err != nil {
		return nil, err
	}

	return &paymentv1.SetPaymentMethodResponse{}, nil
}

func (s *PaymentServer) GetEarnings(ctx context.Context, _ *paymentv1.GetEarningsRequest) (*paymentv1.GetEarningsResponse, error) {
	balances, err := s.service.Earnings(ctx, grpcserver.CallerFrom(ctx).UserID)
	if err != nil {
		return nil, err
	}

	res := &paymentv1.GetEarningsResponse{}
	for _, m := range balances {
		res.Balances = append(res.Balances, toMoneyProto(m))
	}
	return res, nil
}

func toMoneyProto(m payment.Money) *paymentv1.Money {
	return &paymentv1.Money{Amount: m.Amount, Currency: m.Currency}
}
