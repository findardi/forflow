package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"forflow/internal/store/sqlc"
)

const (
	// Partitions exist this many days ahead, so maintenance can fail for days
	// before an attempt insert finds no partition.
	partitionDaysAhead = 3
	partitionInterval  = time.Hour
)

// maintainPartitions creates upcoming attempts partitions and drops the ones
// older than retentionDays.
func maintainPartitions(ctx context.Context, q *sqlc.Queries, retentionDays int32) error {
	// Create before drop: a failed drop only costs disk, a missing partition
	// fails inserts.
	if err := q.EnsureAttemptPartitions(ctx, partitionDaysAhead); err != nil {
		return fmt.Errorf("ensure attempt partitions: %w", err)
	}
	if err := q.DropAttemptPartitions(ctx, retentionDays); err != nil {
		return fmt.Errorf("drop attempt partitions: %w", err)
	}
	return nil
}

// runPartitionMaintenance repeats maintainPartitions every partitionInterval
// until ctx is cancelled. Errors are logged, not returned: a lock timeout is
// transient and partitionDaysAhead leaves days to recover.
func runPartitionMaintenance(ctx context.Context, q *sqlc.Queries, retentionDays int32, log *slog.Logger) {
	t := time.NewTicker(partitionInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := maintainPartitions(ctx, q, retentionDays); err != nil && ctx.Err() == nil {
				log.Error("maintain partitions", "err", err)
			}
		}
	}
}
