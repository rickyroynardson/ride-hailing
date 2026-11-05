package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvOnly(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("SERVICE_NAME", "prod-svc")
	t.Setenv("REDIS_URL", "redis://prod")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "prod-svc" || cfg.RedisURL != "redis://prod" {
		t.Fatalf("got %+v", cfg)
	}
}

func TestLoadEnvYaml(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("service_name: yaml-svc\nredis_url: redis://yaml"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "yaml-svc" || cfg.RedisURL != "redis://yaml" {
		t.Fatalf("got %+v", cfg)
	}
}
