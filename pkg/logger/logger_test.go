package logger

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// captureExporter keeps exported OpenTelemetry log records in memory.
type captureExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *captureExporter) Export(_ context.Context, rs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rs {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (e *captureExporter) Shutdown(context.Context) error   { return nil }
func (e *captureExporter) ForceFlush(context.Context) error { return nil }

// TestLogsCarryTrace checks both sides of a log line written inside a request: the OpenTelemetry record has the
// request's trace and span IDs, and stdout has the line without the context field. The logger is built before the
// provider is installed, as in main, so this also checks the late binding.
func TestLogsCarryTrace(t *testing.T) {
	var stdout bytes.Buffer
	lg, err := build("test-service", "production", zapcore.AddSync(&stdout))
	if err != nil {
		t.Fatal(err)
	}

	exp := &captureExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	otel.SetLoggerProvider(lp)

	ctx, span := sdktrace.NewTracerProvider().Tracer("t").Start(context.Background(), "request")
	lg.Warn("rpc", zap.String("code", "Unavailable"), zap.Any("ctx", ctx))
	span.End()
	lp.ForceFlush(context.Background())

	if len(exp.records) != 1 {
		t.Fatalf("want 1 OpenTelemetry record, got %d", len(exp.records))
	}
	r := exp.records[0]
	if r.TraceID() != span.SpanContext().TraceID() || r.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("record trace %s span %s, want %s %s", r.TraceID(), r.SpanID(), span.SpanContext().TraceID(), span.SpanContext().SpanID())
	}
	if line := stdout.String(); !strings.Contains(line, `"code":"Unavailable"`) || strings.Contains(line, `"ctx"`) {
		t.Fatalf("stdout line should keep fields but not the context: %s", line)
	}

	// Below the logger's level, nothing reaches OpenTelemetry either.
	lg.Debug("noise", zap.Any("ctx", ctx))
	lp.ForceFlush(context.Background())
	if len(exp.records) != 1 {
		t.Fatalf("debug record exported at production level: %d records", len(exp.records))
	}
}
