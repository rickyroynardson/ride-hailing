package grpcserver

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

// TestTracingPropagation checks the wiring every service relies on: a client and a server set up with
// ClientTracing and ServerTracing record spans of one trace, the server span is the client span's child, and the
// server's log line carries that trace ID.
func TestTracingPropagation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	core, logs := observer.New(zap.DebugLevel)

	gs := grpc.NewServer(ServerTracing(), grpc.ChainUnaryInterceptor(Logging(zap.New(core), nil)))
	healthpb.RegisterHealthServer(gs, health.NewServer())
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	defer gs.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		ClientTracing(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	gs.GracefulStop() // let the server span end before reading it

	var client, server sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindClient:
			client = s
		case trace.SpanKindServer:
			server = s
		}
	}
	if client == nil || server == nil {
		t.Fatalf("want a client and a server span, got %d spans", len(recorder.Ended()))
	}
	if server.SpanContext().TraceID() != client.SpanContext().TraceID() {
		t.Fatal("server span started a new trace instead of continuing the client's")
	}
	if server.Parent().SpanID() != client.SpanContext().SpanID() {
		t.Fatal("server span is not the client span's child")
	}

	entries := logs.All()
	if len(entries) != 1 || entries[0].ContextMap()["trace_id"] != client.SpanContext().TraceID().String() {
		t.Fatalf("log line should carry trace_id %s, got %v", client.SpanContext().TraceID(), entries)
	}
}
