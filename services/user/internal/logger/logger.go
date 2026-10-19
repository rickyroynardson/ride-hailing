package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(service, env string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if env == "development" {
		cfg = zap.NewDevelopmentConfig()
	}

	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.InitialFields = map[string]any{"service": service}

	return cfg.Build()
}
