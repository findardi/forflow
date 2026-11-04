package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadBreakerDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://placeholder")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.BreakerThreshold != 5 || cfg.BreakerCooldown != 30*time.Second ||
		cfg.BreakerMaxCooldown != time.Hour || cfg.EndpointDisableAfter != 120*time.Hour {
		t.Errorf("breaker defaults = %d %v %v %v, want 5 30s 1h 120h",
			cfg.BreakerThreshold, cfg.BreakerCooldown, cfg.BreakerMaxCooldown, cfg.EndpointDisableAfter)
	}
}

func TestLoadRejectsBadBreakerConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"zero threshold", map[string]string{"BREAKER_THRESHOLD": "0"}, "BREAKER_THRESHOLD"},
		{"duration without unit", map[string]string{"BREAKER_COOLDOWN": "30"}, "BREAKER_COOLDOWN"},
		{"zero cooldown", map[string]string{"BREAKER_COOLDOWN": "0s"}, "BREAKER_COOLDOWN"},
		{"negative disable", map[string]string{"ENDPOINT_DISABLE_AFTER": "-1h"}, "ENDPOINT_DISABLE_AFTER"},
		{"zero disable", map[string]string{"ENDPOINT_DISABLE_AFTER": "0s"}, "ENDPOINT_DISABLE_AFTER"},
		{"max below base", map[string]string{"BREAKER_COOLDOWN": "2m", "BREAKER_MAX_COOLDOWN": "1m"}, "BREAKER_MAX_COOLDOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://placeholder")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Load error = %v, want one naming %s", err, tt.wantErr)
			}
		})
	}
}
