// Package app wires dependencies and owns the process lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"forflow/internal/config"
	"forflow/internal/delivery"
	"forflow/internal/httpapi"
	"forflow/internal/message"
	"forflow/internal/store/sqlc"
)

const shutdownTimeout = 10 * time.Second

// RunAPI serves the HTTP API until ctx is cancelled, then drains
// in-flight requests for at most shutdownTimeout.
func RunAPI(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, err := NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	// Deferred, so the pool closes only after Shutdown has drained requests.
	defer pool.Close()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewRouter(sqlc.New(pool), message.NewService(pool), log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("serve http: %w", err)
	case <-ctx.Done():
	}

	// ctx is already cancelled, so shutdown needs a fresh deadline of its own.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve http: %w", err)
	}
	return nil
}

// RunWorker runs the delivery worker until ctx is cancelled.
func RunWorker(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, err := NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// ponytail: hardcoded v0 values; move to config when they need tuning per deploy.
	// Lease must exceed HTTPTimeout, or slow but healthy attempts get delivered twice.
	w := delivery.New(pool, delivery.Config{
		HTTPTimeout:  30 * time.Second,
		Lease:        60 * time.Second,
		PollInterval: time.Second,
	})

	log.Info("worker started")
	if err := w.Run(ctx); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}
	log.Info("worker stopped")
	return nil
}
