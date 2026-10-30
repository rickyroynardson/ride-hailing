// Package logger builds a service's zap logger: JSON (or console in development) to stdout, plus every record sent
// as an OpenTelemetry log once telemetry.Setup has configured an exporter.
package logger

import (
	"context"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(service, env string) (*zap.Logger, error) {
	return build(service, env, nil)
}

// build lets tests capture the stdout side; nil means the config's own output.
func build(service, env string, out zapcore.WriteSyncer) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if env == "development" {
		cfg = zap.NewDevelopmentConfig()
	}

	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.InitialFields = map[string]any{"service": service}
	cfg.DisableStacktrace = true

	// The OpenTelemetry side uses the global logger provider, which telemetry.Setup installs later; until then, and
	// whenever tracing is off, it sends nothing. A context.Context field (zap.Any("ctx", ctx)) gives the record its
	// trace_id and span_id, so logs and traces link both ways.
	otelCore := atLeast{otelzap.NewCore(service), cfg.Level}
	opts := []zap.Option{zap.WrapCore(func(stdout zapcore.Core) zapcore.Core {
		if out != nil {
			stdout = zapcore.NewCore(zapcore.NewJSONEncoder(cfg.EncoderConfig), out, cfg.Level).With([]zap.Field{zap.String("service", service)})
		}
		return zapcore.NewTee(withoutContext{stdout}, otelCore)
	})}
	return cfg.Build(opts...)
}

// atLeast applies the logger's level to the OpenTelemetry side. zapcore.NewIncreaseLevelCore can't be used: it
// validates against the bridge's current state, which reports every level disabled until a provider is installed.
type atLeast struct {
	zapcore.Core
	level zapcore.LevelEnabler
}

func (c atLeast) Enabled(l zapcore.Level) bool { return c.level.Enabled(l) && c.Core.Enabled(l) }

func (c atLeast) With(fields []zapcore.Field) zapcore.Core {
	return atLeast{c.Core.With(fields), c.level}
}

func (c atLeast) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if !c.level.Enabled(e.Level) {
		return ce
	}
	return c.Core.Check(e, ce)
}

// withoutContext drops context.Context fields from the stdout output, where they would print as noise; the
// OpenTelemetry side reads them for trace correlation.
type withoutContext struct{ zapcore.Core }

func (c withoutContext) With(fields []zapcore.Field) zapcore.Core {
	return withoutContext{c.Core.With(dropContext(fields))}
}

func (c withoutContext) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c withoutContext) Write(e zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(e, dropContext(fields))
}

func dropContext(fields []zapcore.Field) []zapcore.Field {
	kept := fields[:0:0]
	for _, f := range fields {
		if _, ok := f.Interface.(context.Context); !ok {
			kept = append(kept, f)
		}
	}
	return kept
}
