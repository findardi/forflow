package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"forflow/internal/app"
	"forflow/internal/config"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.RunWorker(ctx, cfg, log); err != nil {
		log.Error("worker exited", "err", err)
		os.Exit(1)
	}
}
