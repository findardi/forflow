//go:build integration

package delivery_test

// The partition functions live in migrations/00010_attempts.sql. They are
// tested here because this package already runs every migration against a
// real Postgres.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const dropAllAttemptPartitions = `DO $$
	DECLARE p text;
	BEGIN
		FOR p IN SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		         WHERE i.inhparent = 'attempts'::regclass LOOP
			EXECUTE format('DROP TABLE %I', p);
		END LOOP;
	END $$`

func txExec(t *testing.T, tx pgx.Tx, sql string) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// partitionBounds maps each attempts partition to its bound expression.
func partitionBounds(t *testing.T, tx pgx.Tx) map[string]string {
	t.Helper()
	rows, err := tx.Query(context.Background(), `
		SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
		FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'attempts'::regclass`)
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}
	got := map[string]string{}
	var name, bound string
	if _, err := pgx.ForEachRow(rows, []any{&name, &bound}, func() error {
		got[name] = bound
		return nil
	}); err != nil {
		t.Fatalf("scan partitions: %v", err)
	}
	return got
}

// The whole test runs in one transaction: now() is fixed for its duration, so
// "today" cannot move under the test at midnight, and the rollback leaves the
// migration's partitions for the other tests.
func TestAttemptPartitionLifecycle(t *testing.T) {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var today time.Time
	if err := tx.QueryRow(ctx, `SELECT (now() AT TIME ZONE 'UTC')::date`).Scan(&today); err != nil {
		t.Fatalf("today: %v", err)
	}
	day := func(offset int) string { return today.AddDate(0, 0, offset).Format("2006-01-02") }
	name := func(offset int) string { return "attempts_" + strings.ReplaceAll(day(offset), "-", "") }

	// Start from nothing, so the test proves ensure creates partitions rather
	// than finding the migration's.
	txExec(t, tx, dropAllAttemptPartitions)

	// A non-UTC zone and DMY dates must not shift the bounds.
	txExec(t, tx, `SET LOCAL timezone = 'Asia/Jakarta'`)
	txExec(t, tx, `SET LOCAL datestyle = 'SQL, DMY'`)
	txExec(t, tx, `SELECT ensure_attempt_partitions(3)`)
	txExec(t, tx, `SELECT ensure_attempt_partitions(3)`) // idempotent
	// pg_get_expr renders bounds in the session zone; read them back in UTC.
	txExec(t, tx, `SET LOCAL timezone = 'UTC'`)
	txExec(t, tx, `SET LOCAL datestyle = 'ISO'`)

	got := partitionBounds(t, tx)
	if len(got) != 5 {
		t.Errorf("got %d partitions %v, want yesterday through today+3", len(got), got)
	}
	for offset := -1; offset <= 3; offset++ {
		want := fmt.Sprintf("FOR VALUES FROM ('%s 00:00:00+00') TO ('%s 00:00:00+00')", day(offset), day(offset+1))
		if got[name(offset)] != want {
			t.Errorf("%s = %q, want %q", name(offset), got[name(offset)], want)
		}
	}

	// Retention of 30 days keeps today-30 and drops anything older.
	for _, offset := range []int{-31, -30} {
		txExec(t, tx, fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF attempts FOR VALUES FROM ('%s 00:00+00') TO ('%s 00:00+00')`,
			name(offset), day(offset), day(offset+1)))
	}
	txExec(t, tx, `SELECT drop_attempt_partitions(30)`)

	got = partitionBounds(t, tx)
	if _, ok := got[name(-31)]; ok {
		t.Errorf("%s survived retention of 30 days", name(-31))
	}
	for _, offset := range []int{-30, -1, 0, 3} {
		if _, ok := got[name(offset)]; !ok {
			t.Errorf("%s was dropped, want kept", name(offset))
		}
	}
}

// keep_days 0 would drop today's partition, which is data loss.
func TestDropAttemptPartitionsRejectsZero(t *testing.T) {
	_, err := pool.Exec(context.Background(), `SELECT drop_attempt_partitions(0)`)
	if err == nil || !strings.Contains(err.Error(), "keep_days must be >= 1") {
		t.Errorf("drop_attempt_partitions(0) error = %v, want keep_days guard", err)
	}
}
