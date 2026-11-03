package config

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	AppEnv string `mapstructure:"app_env"`
	// APIPort serves the public API. HTTPPort serves /metrics and the health check, and is never exposed publicly.
	APIPort     string `mapstructure:"api_port"`
	HTTPPort    string `mapstructure:"http_port"`
	ServiceName string `mapstructure:"service_name"`
	RedisURL    string `mapstructure:"redis_url"`
	// The services behind the gateway; the defaults are the compose service names.
	UserAddr     string `mapstructure:"user_addr"`
	RideAddr     string `mapstructure:"ride_addr"`
	LocationAddr string `mapstructure:"location_addr"`
	MatchingAddr string `mapstructure:"matching_addr"`
	PaymentAddr  string `mapstructure:"payment_addr"`
	// CORSOrigins are the browser origins allowed to call the API, comma separated. Empty allows none, which is fine
	// for mobile apps: CORS only binds browsers.
	CORSOrigins []string `mapstructure:"cors_origins"`
	// Requests allowed per minute: per client IP on the auth routes, per access token elsewhere, and per client IP
	// for requests without a token.
	AuthLimit int `mapstructure:"auth_limit_per_minute"`
	UserLimit int `mapstructure:"user_limit_per_minute"`
	AnonLimit int `mapstructure:"anon_limit_per_minute"`
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("app_env", "production")
	v.SetDefault("api_port", "8000")
	v.SetDefault("http_port", "8080")
	v.SetDefault("service_name", "gateway")
	v.SetDefault("redis_url", "")
	v.SetDefault("user_addr", "user:50051")
	v.SetDefault("ride_addr", "ride:50051")
	v.SetDefault("location_addr", "location:50051")
	v.SetDefault("matching_addr", "matching:50051")
	v.SetDefault("payment_addr", "payment:50051")
	v.SetDefault("cors_origins", "")
	v.SetDefault("auth_limit_per_minute", 10)
	v.SetDefault("user_limit_per_minute", 300)
	v.SetDefault("anon_limit_per_minute", 60)

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
	cfg.CORSOrigins = splitList(cfg.CORSOrigins)

	if cfg.RedisURL == "" {
		return nil, errors.New("redis_url is required")
	}
	if cfg.AuthLimit <= 0 || cfg.UserLimit <= 0 || cfg.AnonLimit <= 0 {
		return nil, errors.New("rate limits must be positive")
	}

	return &cfg, nil
}

// splitList accepts "a,b" from an env var as well as a YAML list, and drops blanks.
func splitList(in []string) []string {
	var out []string
	for _, s := range in {
		for _, part := range strings.Split(s, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}
