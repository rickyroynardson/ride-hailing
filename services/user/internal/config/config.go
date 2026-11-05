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
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("app_env", "production")
	v.SetDefault("http_port", "8080")
	v.SetDefault("grpc_port", "50051")
	v.SetDefault("service_name", "user-service")
	v.SetDefault("postgres_url", "")

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

	return &cfg, nil
}
