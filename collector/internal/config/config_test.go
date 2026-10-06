package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_EnvExpansionAndDefaults(t *testing.T) {
	t.Setenv("TEST_TINKOFF_TOKEN", "tok123")
	t.Setenv("TEST_ADMIN_KEY", "admin123")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
tinkoff:
  endpoint: "invest-public-api.tinkoff.ru:443"
  token: "${TEST_TINKOFF_TOKEN}"

http:
  admin_api_key: "${TEST_ADMIN_KEY}"

subscriptions:
  initial:
    - timeframe: "1min"
      instruments: [SBER, GAZP]
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Tinkoff.Token != "tok123" {
		t.Fatalf("expected env var expansion for token, got %q", cfg.Tinkoff.Token)
	}
	if cfg.HTTP.AdminAPIKey != "admin123" {
		t.Fatalf("expected env var expansion for admin key, got %q", cfg.HTTP.AdminAPIKey)
	}
	if cfg.Tinkoff.EndPoint != "invest-public-api.tinkoff.ru:443" {
		t.Fatalf("unexpected endpoint: %q", cfg.Tinkoff.EndPoint)
	}
	if cfg.Subscriptions.MaxPerStream != 300 {
		t.Fatalf("expected default max_per_stream=300, got %d", cfg.Subscriptions.MaxPerStream)
	}
	if cfg.HTTP.Addr != ":8081" {
		t.Fatalf("expected default http addr :8081, got %q", cfg.HTTP.Addr)
	}
	if len(cfg.Subscriptions.Initial) != 1 || len(cfg.Subscriptions.Initial[0].Instruments) != 2 {
		t.Fatalf("unexpected initial subscriptions: %+v", cfg.Subscriptions.Initial)
	}
}

func TestLoad_MissingRequiredFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("tinkoff: {}\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("expected error for missing token/admin_api_key, got nil")
	}
}
