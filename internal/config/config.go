// Package config loads process configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is shared by the api and worker processes.
type Config struct {
	HTTPAddr    string
	DatabaseURL string
	// WorkerCount is how many deliveries the worker sends at once.
	WorkerCount int
	// WorkerMaxInFlight caps concurrent deliveries per endpoint, so a hanging
	// endpoint holds at most this many of the WorkerCount slots.
	WorkerMaxInFlight int
	// AttemptsRetentionDays is how many past days of attempts are kept besides
	// today; older daily partitions are dropped.
	AttemptsRetentionDays int
	// BreakerThreshold is the failure streak that opens an endpoint's breaker.
	BreakerThreshold int
	// BreakerCooldown is the first pause of an open breaker; each further
	// failure doubles it, up to BreakerMaxCooldown.
	BreakerCooldown    time.Duration
	BreakerMaxCooldown time.Duration
	// EndpointDisableAfter is how long an endpoint may fail without a single
	// success before it is disabled.
	EndpointDisableAfter time.Duration
}

// Load reads the environment and fails fast on missing required values,
// so a misconfigured process never starts half-working.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("config: DATABASE_URL is required")
	}

	var err error
	if cfg.WorkerCount, err = getEnvPositiveInt("WORKER_COUNT", 50); err != nil {
		return Config{}, err
	}
	if cfg.WorkerMaxInFlight, err = getEnvPositiveInt("WORKER_MAX_IN_FLIGHT", 4); err != nil {
		return Config{}, err
	}
	if cfg.AttemptsRetentionDays, err = getEnvPositiveInt("ATTEMPTS_RETENTION_DAYS", 30); err != nil {
		return Config{}, err
	}
	// Defaults: 30s first pause as in Convoy; disable after 5 days of failing,
	// as in Svix (endpoint_failure_disable_after = 120h).
	if cfg.BreakerThreshold, err = getEnvPositiveInt("BREAKER_THRESHOLD", 5); err != nil {
		return Config{}, err
	}
	if cfg.BreakerCooldown, err = getEnvPositiveDuration("BREAKER_COOLDOWN", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.BreakerMaxCooldown, err = getEnvPositiveDuration("BREAKER_MAX_COOLDOWN", time.Hour); err != nil {
		return Config{}, err
	}
	if cfg.BreakerMaxCooldown < cfg.BreakerCooldown {
		return Config{}, fmt.Errorf("config: BREAKER_MAX_COOLDOWN (%v) must not be below BREAKER_COOLDOWN (%v)",
			cfg.BreakerMaxCooldown, cfg.BreakerCooldown)
	}
	if cfg.EndpointDisableAfter, err = getEnvPositiveDuration("ENDPOINT_DISABLE_AFTER", 120*time.Hour); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// getEnvPositiveInt rejects zero: a zero worker count or per-endpoint cap
// starts a worker that silently never delivers anything. Values are bounded
// to int32 because the cap is passed to Postgres as an int.
func getEnvPositiveInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("config: %s must be a positive integer, got %q", key, v)
	}
	return int(n), nil
}

// getEnvPositiveDuration rejects zero: a zero ENDPOINT_DISABLE_AFTER disables
// an endpoint on its second failure, and a zero cooldown never pauses at all.
func getEnvPositiveDuration(key string, fallback time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("config: %s must be a positive duration such as 30s or 120h, got %q", key, v)
	}
	return d, nil
}
