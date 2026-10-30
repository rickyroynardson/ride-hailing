// Package telemetry sets up OpenTelemetry tracing for a service.
package telemetry

import (
	"context"
	"crypto/rand"
	"errors"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Setup installs the global tracer and logger providers and returns a function that flushes spans still buffered; call it on
// shutdown. Configuration uses the standard OpenTelemetry environment variables:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT  where to send spans and logs, e.g. http://otel-collector:4317. Unset: both are off
//	                             (tests, go run); logs still go to stdout.
//	OTEL_TRACES_SAMPLER(_ARG)    sampling; the default records every trace, fine for development. In production,
//	                             e.g. parentbased_traceidratio with 0.1 keeps 10% of traces, whole traces at a time.
//
// The W3C trace-context propagator is installed either way, so a service with tracing off still passes the trace
// through to the services it calls.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", service)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, err
	}

	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)

	// Logs go the same way as traces, so the backend can show a request's logs next to its trace. The zap logger
	// (pkg/logger) writes through this provider.
	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)), sdklog.WithResource(res))
	otel.SetLoggerProvider(lp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

// Untraced marks ctx as "don't record". Spans started under it, including the automatic ones for SQL, Redis and
// outgoing gRPC, inherit the decision through the default parent-based sampler and are dropped. Background loops
// run under it, so polling ("anything to do?") never fills the trace backend.
func Untraced(ctx context.Context) context.Context {
	var tid trace.TraceID
	var sid trace.SpanID
	rand.Read(tid[:])
	rand.Read(sid[:])
	return trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid}))
}

// TraceParent returns the current trace context as a W3C traceparent string, to store with work that continues
// later, e.g. an outbox message (like a message header on a queue). Empty when ctx carries no trace. The sampled
// flag goes with it, so follow-up work is recorded exactly when the original request was.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// remote returns ctx carrying the trace stored in traceParent, or ctx unchanged when it is empty or malformed.
func remote(ctx context.Context, traceParent string) (trace.SpanContext, bool) {
	if traceParent == "" {
		return trace.SpanContext{}, false
	}
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceParent}))
	return sc, sc.IsValid()
}

// StartWork begins a span for one piece of background work, e.g. sending an outbox message or re-offering a ride.
// With a stored traceParent it continues that trace, so the work appears inside the request that caused it;
// without one it starts a new trace. Either way it ignores the Untraced loop context it runs under. Call end with
// the work's error.
func StartWork(ctx context.Context, name, traceParent string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return start(ctx, name, traceParent, trace.SpanKindInternal, attrs)
}

func start(ctx context.Context, name, traceParent string, kind trace.SpanKind, attrs []attribute.KeyValue) (context.Context, func(error)) {
	opts := []trace.SpanStartOption{trace.WithSpanKind(kind), trace.WithAttributes(attrs...)}
	if sc, ok := remote(ctx, traceParent); ok {
		ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	} else {
		opts = append(opts, trace.WithNewRoot())
	}
	ctx, span := otel.Tracer("work").Start(ctx, name, opts...)
	return ctx, endFunc(span)
}

func endFunc(span trace.Span) func(error) {
	return func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// StartProducer begins a PRODUCER span where async work is queued, e.g. writing an outbox message inside a request.
// Store TraceParent of the returned context with the message, so the consumer's span becomes its child: the trace
// UI then shows an async hop (queued here, processed later) instead of a child that oddly starts after its parent
// ended. Follows the OpenTelemetry messaging conventions.
func StartProducer(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	ctx, span := otel.Tracer("work").Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
	return ctx, endFunc(span)
}

// StartConsumer begins a CONSUMER span processing a queued message, continuing the trace stored by the producer
// (see StartWork for how traceParent is used).
func StartConsumer(ctx context.Context, name, traceParent string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return start(ctx, name, traceParent, trace.SpanKindConsumer, attrs)
}

// LinkTo links the current span to the trace in traceParent. Use it when work is related but started by someone
// else, e.g. a driver accepting a rider's request: separate traces, connected both ways in the trace UI.
func LinkTo(ctx context.Context, traceParent string) {
	if sc, ok := remote(ctx, traceParent); ok {
		trace.SpanFromContext(ctx).AddLink(trace.Link{SpanContext: sc})
	}
}

// Tag adds attributes to the current span, typically business IDs (ride.id, offer.id) so traces can be found by
// what they are about.
func Tag(ctx context.Context, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).SetAttributes(attrs...)
}
