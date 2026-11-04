// Package config loads process configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
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
