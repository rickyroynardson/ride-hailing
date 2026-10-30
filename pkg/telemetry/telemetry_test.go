package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestUntracedAndStartWork checks what reaches the trace backend from background loops: nothing that happens
// under Untraced (polling queries, automatic spans), but every StartWork, as a trace of its own.
func TestUntracedAndStartWork(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	// The default sampler, as in Setup: follow the parent's decision, record new roots.
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	tracer := otel.Tracer("test")

	ctx := Untraced(context.Background())
	_, polling := tracer.Start(ctx, "SELECT outbox")
	polling.End()

	workCtx, end := StartWork(ctx, "outbox send start_matching", "")
	_, query := tracer.Start(workCtx, "UPDATE outbox")
	query.End()
	end(nil)

	spans := recorder.Ended()
	names := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		names[s.Name()] = s
	}
	if _, ok := names["SELECT outbox"]; ok {
		t.Fatal("span under Untraced was recorded")
	}
	work, child := names["outbox send start_matching"], names["UPDATE outbox"]
	if work == nil || child == nil || len(spans) != 2 {
		t.Fatalf("want the work span and its query, got %d spans", len(spans))
	}
	if work.Parent().IsValid() {
		t.Fatal("StartWork should begin a new trace, not continue the untraced context")
	}
	if child.Parent().SpanID() != work.SpanContext().SpanID() {
		t.Fatal("query span is not inside the work span")
	}
}

// TestTraceParentRoundTrip checks the outbox path: a request stores its trace context, and work started later from
// the stored string joins the same trace as a child of the request, even under Untraced.
func TestTraceParentRoundTrip(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	tracer := otel.Tracer("test")

	reqCtx, req := tracer.Start(context.Background(), "CreateRide")
	stored := TraceParent(reqCtx)
	req.End()
	if stored == "" {
		t.Fatal("no traceparent for a traced request")
	}
	if TraceParent(context.Background()) != "" {
		t.Fatal("traceparent outside any trace should be empty")
	}

	_, end := StartWork(Untraced(context.Background()), "outbox send start_matching", stored)
	end(nil)

	var work sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "outbox send start_matching" {
			work = s
		}
	}
	if work == nil {
		t.Fatal("work span not recorded")
	}
	if work.SpanContext().TraceID() != req.SpanContext().TraceID() || work.Parent().SpanID() != req.SpanContext().SpanID() {
		t.Fatal("work did not continue the request's trace as its child")
	}

	// A malformed stored value falls back to a new trace instead of failing the work.
	_, end = StartWork(context.Background(), "bad parent", "not-a-traceparent")
	end(nil)

	// LinkTo: a separate trace pointing back at the request.
	linkCtx, linked := tracer.Start(context.Background(), "AcceptOffer")
	LinkTo(linkCtx, stored)
	linked.End()
	for _, s := range recorder.Ended() {
		if s.Name() == "AcceptOffer" {
			if s.SpanContext().TraceID() == req.SpanContext().TraceID() {
				t.Fatal("linked work should be its own trace")
			}
			if len(s.Links()) != 1 || s.Links()[0].SpanContext.TraceID() != req.SpanContext().TraceID() {
				t.Fatalf("AcceptOffer links: %v", s.Links())
			}
		}
	}
}

// TestProducerConsumer checks the async hop: the consumer is a CONSUMER child of the PRODUCER span whose context
// was stored, not of the request.
func TestProducerConsumer(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))

	reqCtx, req := otel.Tracer("test").Start(context.Background(), "CreateRide")
	pubCtx, endPub := StartProducer(reqCtx, "outbox publish start_matching")
	stored := TraceParent(pubCtx)
	endPub(nil)
	req.End()

	_, endProc := StartConsumer(Untraced(context.Background()), "outbox process start_matching", stored)
	endProc(nil)

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range recorder.Ended() {
		byName[s.Name()] = s
	}
	pub, proc := byName["outbox publish start_matching"], byName["outbox process start_matching"]
	if pub == nil || proc == nil {
		t.Fatalf("spans: %v", byName)
	}
	if pub.SpanKind() != trace.SpanKindProducer || proc.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("kinds: publish %v, process %v", pub.SpanKind(), proc.SpanKind())
	}
	if pub.Parent().SpanID() != req.SpanContext().SpanID() {
		t.Fatal("producer is not inside the request")
	}
	if proc.Parent().SpanID() != pub.SpanContext().SpanID() || proc.SpanContext().TraceID() != req.SpanContext().TraceID() {
		t.Fatal("consumer is not the producer's child in the request's trace")
	}
}
