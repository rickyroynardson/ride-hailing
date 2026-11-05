// Deploymentcheck is a disposable two-replica fixture, not a production payment server or admin API.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const charge = "/payment.v1.PaymentService/ChargeRide"
const watch = "/fixture.v1.Deployment/Watch"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: deploymentcheck serve|exercise|verify [request-id]")
	}
	pool, err := pgxpool.New(context.Background(), os.Getenv("STORE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if os.Args[1] == "serve" {
		err = serve(pool)
	} else if len(os.Args) == 3 {
		err = exercise(pool, os.Args[1], os.Args[2])
	} else {
		err = errors.New("request ID required")
	}
	if err != nil {
		log.Fatal(err)
	}
}

type fixture struct {
	pool *pgxpool.Pool
	boot string
}
type chargeServer interface {
	Charge(context.Context, *structpb.Struct) (*structpb.Struct, error)
}

func (f *fixture) Charge(ctx context.Context, in *structpb.Struct) (*structpb.Struct, error) {
	id := in.GetFields()["request_id"].GetStringValue()
	if id == "" || len(id) > 128 {
		return nil, status.Error(codes.InvalidArgument, "fixture request_id required")
	}
	var listener *pgx.Conn
	if in.GetFields()["hold"].GetBoolValue() {
		var err error
		listener, err = pgx.Connect(ctx, os.Getenv("STORE_URL"))
		if err != nil {
			return nil, err
		}
		defer listener.Close(context.Background())
		if _, err = listener.Exec(ctx, "LISTEN deployment_release"); err != nil {
			return nil, err
		}
	}
	var first string
	var effects int
	err := f.pool.QueryRow(ctx, `INSERT INTO deployment_effects(request_id,first_replica) VALUES($1,$2)
		ON CONFLICT(request_id) DO UPDATE SET attempts=deployment_effects.attempts+1
		RETURNING first_replica,effects`, id, f.boot).Scan(&first, &effects)
	if err != nil {
		return nil, err
	}
	// The durable side effect precedes the acknowledgement. Headers synchronize the drill without sleeps.
	if err = grpc.SendHeader(ctx, metadata.Pairs("fixture-committed", "true")); err != nil {
		return nil, err
	}
	if listener != nil {
		for {
			event, err := listener.WaitForNotification(ctx)
			if err != nil {
				return nil, err
			}
			if event.Payload == id {
				break
			}
		}
	}
	if in.GetFields()["lose_ack"].GetBoolValue() {
		return nil, status.Error(codes.Unavailable, "fixture committed; reply lost")
	}
	return structpb.NewStruct(map[string]any{"first_replica": first, "current_replica": f.boot, "effects": effects})
}

func serve(pool *pgxpool.Pool) error {
	boot := make([]byte, 16)
	if _, err := rand.Read(boot); err != nil {
		return err
	}
	f := &fixture{pool: pool, boot: hex.EncodeToString(boot)}
	life := lifecycle.New(pool.Ping)
	transport, err := workloadtls.ServerFromEnv()
	if err != nil {
		return err
	}
	verifier, err := jwtauth.VerifierFromEnv("payment")
	if err != nil {
		return err
	}
	access := map[string]grpcserver.Access{charge: grpcserver.ServicesOnly, watch: grpcserver.Public}
	gs := grpc.NewServer(transport, life.UnaryOption(), life.StreamOption(), grpc.ChainUnaryInterceptor(grpcserver.Logging(zap.NewNop(), nil), grpcserver.Auth(verifier, access)), grpc.ChainStreamInterceptor(grpcserver.StreamAuth(verifier, access)))
	gs.RegisterService(&grpc.ServiceDesc{ServiceName: "payment.v1.PaymentService", HandlerType: (*chargeServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "ChargeRide", Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := &structpb.Struct{}
		if err := decode(in); err != nil {
			return nil, err
		}
		return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: charge}, func(ctx context.Context, req any) (any, error) {
			return srv.(chargeServer).Charge(ctx, req.(*structpb.Struct))
		})
	}}}}, f)
	gs.RegisterService(&grpc.ServiceDesc{ServiceName: "fixture.v1.Deployment", HandlerType: (*any)(nil), Streams: []grpc.StreamDesc{{StreamName: "Watch", ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
		if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		message, _ := structpb.NewStruct(map[string]any{"replica": f.boot})
		if err := stream.SendMsg(message); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}}}}, f)
	mux := http.NewServeMux()
	// TEST FIXTURE ONLY. Production binaries have no HTTP drain/kill endpoint or Docker socket.
	mux.HandleFunc("POST /test/stop", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		syscall.Kill(os.Getpid(), syscall.SIGTERM)
	})
	admin := &http.Server{Addr: ":8080", Handler: life.Health(mux), ReadHeaderTimeout: time.Second}
	life.Go(func(ctx context.Context) { <-ctx.Done(); log.Print("fixture worker stopped") })
	err = life.Run(lifecycle.Config{GRPC: gs, GRPCAddress: ":50051", Admin: admin, DrainDelay: 500 * time.Millisecond, GracePeriod: 2 * time.Second, OnReady: func() { log.Print("fixture ready") }, OnDrain: func() { log.Print("fixture draining") }})
	if err == lifecycle.ErrDrainDeadline {
		log.Print("fixture force-closed long stream within grace period")
		return nil
	}
	return err
}

func exercise(pool *pgxpool.Pool, phase, id string) error {
	if phase != "exercise" && phase != "verify" {
		return errors.New("unknown phase")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dial := func(host string) (*grpc.ClientConn, error) {
		tlsCreds, err := workloadtls.ClientFromEnv("payment")
		if err != nil {
			return nil, err
		}
		jwtCreds, err := jwtauth.CredentialsFromEnv("payment")
		if err != nil {
			return nil, err
		}
		return grpcserver.NewClient(host+":50051", grpc.WithTransportCredentials(tlsCreds), grpc.WithPerRPCCredentials(jwtCreds))
	}
	a, err := dial("deployment-a")
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := dial("deployment-b")
	if err != nil {
		return err
	}
	defer b.Close()
	request := func(requestID string, hold, lose bool) *structpb.Struct {
		value, _ := structpb.NewStruct(map[string]any{"request_id": requestID, "hold": hold, "lose_ack": lose})
		return value
	}
	call := func(conn *grpc.ClientConn) (*structpb.Struct, error) {
		out := &structpb.Struct{}
		err := conn.Invoke(ctx, charge, request(id, false, false), out)
		return out, err
	}
	openWatch := func(conn *grpc.ClientConn) (grpc.ClientStream, error) {
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, watch)
		if err != nil {
			return nil, err
		}
		if err = stream.SendMsg(&emptypb.Empty{}); err != nil {
			return nil, err
		}
		stream.CloseSend()
		if err = stream.RecvMsg(&structpb.Struct{}); err != nil {
			return nil, err
		}
		return stream, nil
	}
	if phase == "verify" {
		for _, conn := range []*grpc.ClientConn{a, b} {
			out, err := call(conn)
			if err != nil {
				return err
			}
			if out.GetFields()["effects"].GetNumberValue() != 1 {
				return errors.New("replay duplicated effect")
			}
			if out.GetFields()["first_replica"].GetStringValue() == out.GetFields()["current_replica"].GetStringValue() {
				return errors.New("original process did not restart")
			}
		}
		if _, err := openWatch(a); err != nil {
			return fmt.Errorf("stream reconnect after restart: %w", err)
		}
		fmt.Println("PASS restarted replica ready; stream reopened; replays still have one durable effect")
		return nil
	}
	stream, err := openWatch(a)
	if err != nil {
		return err
	}
	held, err := a.NewStream(ctx, &grpc.StreamDesc{}, charge)
	if err != nil {
		return err
	}
	if err = held.SendMsg(request(id, true, true)); err != nil {
		return err
	}
	held.CloseSend()
	if _, err = held.Header(); err != nil {
		return err
	}
	httpClient := &http.Client{Timeout: time.Second}
	response, err := httpClient.Post("http://deployment-a:8080/test/stop", "application/json", nil)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		return errors.New("fixture signal was not accepted")
	}
	started := time.Now()
	probe := func(path string, want int) error {
		res, err := httpClient.Get("http://deployment-a:8080" + path)
		if err != nil {
			return err
		}
		res.Body.Close()
		if res.StatusCode != want {
			return fmt.Errorf("%s: status %d, want %d", path, res.StatusCode, want)
		}
		return nil
	}
	// Signal delivery is asynchronous; a few bounded readiness polls synchronize with BeginDrain.
	for {
		err = probe("/readyz", 503)
		if err == nil {
			break
		}
		if time.Since(started) > time.Second {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = probe("/livez", 200); err != nil {
		return err
	}
	fmt.Println("PASS draining replica unready while liveness stays up")
	err = a.Invoke(ctx, charge, request(id+"-rejected", false, false), &structpb.Struct{})
	if status.Code(err) != codes.Unavailable {
		return fmt.Errorf("new draining call was not rejected: %v", err)
	}
	if _, err = call(b); err != nil {
		return fmt.Errorf("surviving replica: %w", err)
	}
	fmt.Println("PASS new work rejected on draining replica; survivor accepts the same idempotency key")
	if _, err = pool.Exec(ctx, "SELECT pg_notify('deployment_release',$1)", id); err != nil {
		return err
	}
	if err = held.RecvMsg(&structpb.Struct{}); status.Code(err) != codes.Unavailable {
		return fmt.Errorf("accepted call did not finish with its controlled lost acknowledgement: %v", err)
	}
	fmt.Println("PASS accepted call finished during drain; committed reply loss is safely replayable")
	if err = stream.RecvMsg(&structpb.Struct{}); err == nil {
		return errors.New("stream outlived forced shutdown")
	}
	if time.Since(started) > 5*time.Second {
		return errors.New("stream shutdown exceeded bound")
	}
	fmt.Println("PASS long stream terminated within the shutdown budget")
	var effects, rejected int
	if err = pool.QueryRow(ctx, "SELECT effects FROM deployment_effects WHERE request_id=$1", id).Scan(&effects); err != nil {
		return err
	}
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM deployment_effects WHERE request_id=$1", id+"-rejected").Scan(&rejected); err != nil {
		return err
	}
	if effects != 1 || rejected != 0 {
		return fmt.Errorf("effects=%d rejected-side-effects=%d", effects, rejected)
	}
	fmt.Println("PASS exactly one durable fixture effect; rejected request created nothing")
	return nil
}
