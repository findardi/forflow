//go:build integration

package message_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"forflow/internal/message"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
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
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Printf("open pool: %v", err)
		return 1
	}
	defer pool.Close()

	if err := migrateUp(ctx); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}
	return m.Run()
}

// migrateUp runs the Up section of every migration in order. It avoids a
// goose dependency in tests; the files are tiny and Glob returns them sorted.
func migrateUp(ctx context.Context) error {
	names, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		return err
	}
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		up, _, _ := strings.Cut(string(b), "-- +goose Down")
		// goose statement markers are SQL comments, so the section runs as is.
		if _, err := pool.Exec(ctx, up); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func reset(t *testing.T) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "TRUNCATE attempts, deliveries, messages, endpoints"); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// listen opens a connection outside the pool, because LISTEN belongs to one
// session and a pooled connection would be handed to other callers.
func listen(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
	if err != nil {
		t.Fatalf("connect listener: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	if _, err := conn.Exec(ctx, "LISTEN forflow_deliveries"); err != nil {
		t.Fatalf("listen: %v", err)
	}
	return conn
}

func TestCreateNotifiesWorkers(t *testing.T) {
	reset(t)
	if _, err := pool.Exec(context.Background(),
		"INSERT INTO endpoints (url, secret) VALUES ($1, $2)",
		"http://example.invalid/hook", []byte("forflow-test-signing-secret-32b!")); err != nil {
		t.Fatalf("seed endpoint: %v", err)
	}
	conn := listen(t)

	_, n, err := message.NewService(pool).Create(context.Background(), []byte(`{"order":1}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if n != 1 {
		t.Fatalf("deliveries = %d, want 1", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := conn.WaitForNotification(ctx); err != nil {
		t.Fatalf("no notification after commit: %v", err)
	}
}

// A disabled endpoint gets no new deliveries; only its existing backlog waits.
func TestCreateSkipsDisabledEndpoints(t *testing.T) {
	reset(t)
	var enabled string
	err := pool.QueryRow(context.Background(), `
		WITH a AS (INSERT INTO endpoints (url, secret) VALUES ('http://a.invalid/', $1) RETURNING id),
		     b AS (INSERT INTO endpoints (url, secret, disabled_at) VALUES ('http://b.invalid/', $1, now()) RETURNING id)
		SELECT a.id::text FROM a, b`, []byte("forflow-test-signing-secret-32b!")).Scan(&enabled)
	if err != nil {
		t.Fatalf("seed endpoints: %v", err)
	}

	msg, n, err := message.NewService(pool).Create(context.Background(), []byte(`{"order":1}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if n != 1 {
		t.Fatalf("deliveries = %d, want 1 (disabled endpoint skipped)", n)
	}
	var got string
	if err := pool.QueryRow(context.Background(),
		`SELECT endpoint_id::text FROM deliveries WHERE message_id = $1`, msg.ID).Scan(&got); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if got != enabled {
		t.Errorf("delivery went to endpoint %s, want the enabled one %s", got, enabled)
	}
}

func TestCreateWithoutDeliveriesDoesNotNotify(t *testing.T) {
	reset(t)
	conn := listen(t)

	_, n, err := message.NewService(pool).Create(context.Background(), []byte(`{"order":1}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if n != 0 {
		t.Fatalf("deliveries = %d, want 0", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	got, err := conn.WaitForNotification(ctx)
	if err == nil {
		t.Fatalf("unexpected notification on %q", got.Channel)
	}
	if ctx.Err() == nil {
		t.Fatalf("wait: %v", err)
	}
}
