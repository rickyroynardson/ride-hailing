package config

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	AppEnv      string `mapstructure:"app_env"`
	ServiceName string `mapstructure:"service_name"`
	PostgresURL string `mapstructure:"postgres_url"`
	JWTSecret   string `mapstructure:"jwt_secret"`
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("app_env", "production")
	v.SetDefault("service_name", "ride-service")
	v.SetDefault("postgres_url", "")
	v.SetDefault("jwt_secret", "")

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
