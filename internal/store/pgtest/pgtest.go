//go:build integration

// Package pgtest runs integration tests against a throwaway Postgres with
// every migration applied.
package pgtest

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Main starts Postgres, stores the pool in *pool, runs the tests and exits.
// Call it from TestMain.
func Main(m *testing.M, pool **pgxpool.Pool) {
	os.Exit(run(m, pool))
}

func run(m *testing.M, pool **pgxpool.Pool) int {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("forflow"),
		postgres.WithUsername("forflow"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("open pool: %v", err)
		return 1
	}
	defer p.Close()

	if err := migrateUp(ctx, p); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}
	*pool = p
	return m.Run()
}

// migrateUp runs the Up section of every migration in order, without a goose
// dependency. The path is found from this file, so any test package can call it.
func migrateUp(ctx context.Context, p *pgxpool.Pool) error {
	_, self, _, _ := runtime.Caller(0)
	names, err := filepath.Glob(filepath.Join(filepath.Dir(self), "../../../migrations/*.sql"))
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return fmt.Errorf("no migrations found")
	}
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		up, _, _ := strings.Cut(string(b), "-- +goose Down")
		if _, err := p.Exec(ctx, up); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(name), err)
		}
	}
	return nil
}
