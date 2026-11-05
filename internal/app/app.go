// Package app wires dependencies and owns the process lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"forflow/internal/config"
	"forflow/internal/dashboard"
	"forflow/internal/delivery"
	"forflow/internal/httpapi"
	"forflow/internal/message"
	"forflow/internal/scheduler"
	"forflow/internal/store/sqlc"
	"forflow/web"
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
		Handler:           routes(sqlc.New(pool), message.NewService(pool), log),
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

// routes puts the dashboard and its static files beside the public API.
// ponytail: no auth on any route yet; the dashboard is for development until
// tenants and API keys exist.
func routes(q *sqlc.Queries, msg *message.Service, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", httpapi.NewRouter(q, msg, log))
	mux.Handle("/dashboard/", dashboard.New(q, log))
	mux.Handle("GET /static/", http.FileServerFS(web.Static))
	return mux
}

// RunWorker runs the delivery worker until ctx is cancelled.
func RunWorker(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, err := NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The first run is fatal, so a missing schema or partition stops startup
	// instead of failing every attempt insert later.
	// ponytail: assumes one worker process; concurrent runs can race on CREATE
	// TABLE IF NOT EXISTS. Add a pg_advisory_xact_lock when scaling out.
	q := sqlc.New(pool)
	retention := int32(cfg.AttemptsRetentionDays)
	if err := maintainPartitions(ctx, q, retention); err != nil {
		return err
	}

	// cancel runs before Wait (defers are LIFO), so maintenance and the
	// listener also stop when Run returns an error, and all finish before the
	// pool closes.
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	wg.Go(func() { runPartitionMaintenance(ctx, q, retention, log) })

	// ponytail: hardcoded v0 values; move to config when they need tuning per deploy.
	// Lease must exceed HTTPTimeout, or slow but healthy attempts get delivered twice.
	// New events arrive through the listener, so PollInterval only bounds how
	// late due retries, expired leases and lost notifications are picked up.
	w := delivery.New(pool, delivery.Config{
		HTTPTimeout:        30 * time.Second,
		Lease:              60 * time.Second,
		PollInterval:       5 * time.Second,
		Logger:             log,
		Workers:            cfg.WorkerCount,
		MaxInFlight:        int32(cfg.WorkerMaxInFlight),
		BreakerThreshold:   int32(cfg.BreakerThreshold),
		BreakerCooldown:    cfg.BreakerCooldown,
		BreakerMaxCooldown: cfg.BreakerMaxCooldown,
		DisableAfter:       cfg.EndpointDisableAfter,
	})
	wg.Go(func() {
		if err := scheduler.Listen(ctx, pool.Config().ConnConfig, w.Wake, log); err != nil {
			log.Error("delivery listener stopped", "error", err)
		}
	})

	log.Info("worker started")
	if err := w.Run(ctx); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}
	log.Info("worker stopped")
	return nil
}
