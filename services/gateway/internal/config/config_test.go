package config

import (
	"fmt"
	"testing"
)

func TestLoadEnvOnly(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("REDIS_URL", "redis://r:6379/0")
	t.Setenv("CORS_ORIGINS", "https://a.example, https://b.example,")
	t.Setenv("AUTH_LIMIT_PER_MINUTE", "5")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(cfg.CORSOrigins); got != "[https://a.example https://b.example]" {
		t.Fatalf("cors origins: %s", got)
	}
	if cfg.AuthLimit != 5 || cfg.UserLimit != 300 || cfg.RideAddr != "ride:50051" {
		t.Fatalf("got %+v", cfg)
	}
}

func TestLoadRequiresRedis(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, err := Load(); err == nil {
		t.Fatal("want an error without redis_url")
	}
}

func TestLoadRefusesZeroLimit(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("REDIS_URL", "redis://r:6379/0")
	t.Setenv("USER_LIMIT_PER_MINUTE", "0")
	if _, err := Load(); err == nil {
		t.Fatal("want an error for a zero limit")
	}
}
