// Package config loads process configuration from environment variables.
package config

import (
	"errors"
	"os"
)

// Config is shared by the api and worker processes.
type Config struct {
	HTTPAddr    string
	DatabaseURL string
}

// Load reads the environment and fails fast on missing required values,
// so a misconfigured process never starts half-working.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    os.Getenv("HTTP_ADDR"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("config: DATABASE_URL is required")
	}
	return cfg, nil
}
