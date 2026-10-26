package config

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	AppEnv      string `mapstructure:"app_env"`
	HTTPPort    string `mapstructure:"http_port"`
	GRPCPort    string `mapstructure:"grpc_port"`
	ServiceName string `mapstructure:"service_name"`
	PostgresURL string `mapstructure:"postgres_url"`
	JWTSecret   string `mapstructure:"jwt_secret"`
	// LocationAddr and RideAddr are the gRPC addresses matching calls; the defaults are the compose service names.
	LocationAddr string `mapstructure:"location_addr"`
	RideAddr     string `mapstructure:"ride_addr"`
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("app_env", "production")
	v.SetDefault("http_port", "8080")
	v.SetDefault("grpc_port", "50051")
	v.SetDefault("service_name", "matching-service")
	v.SetDefault("postgres_url", "")
	v.SetDefault("jwt_secret", "")
	v.SetDefault("location_addr", "location:50051")
	v.SetDefault("ride_addr", "ride:50051")

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, err
		}
	}

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if cfg.PostgresURL == "" {
		return nil, errors.New("postgres_url is required")
	}
	if cfg.JWTSecret == "" {
		return nil, errors.New("jwt_secret is required")
	}

	return &cfg, nil
}
