// Package message ingests events and fans them out to endpoints.
package message

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"forflow/internal/store/sqlc"
)

// Service creates messages together with their deliveries.
type Service struct {
	pool *pgxpool.Pool
}

// NewService returns a Service backed by pool.
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool: pool,
	}
}

// Create stores the message and one pending delivery per endpoint in a single
// transaction, so a message is never saved without its deliveries (and vice versa).
func (s *Service) Create(ctx context.Context, payload []byte) (sqlc.Message, int64, error) {
	var (
		msg sqlc.Message
		n   int64
	)

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)

		var err error
		msg, err = q.CreateMessage(ctx, payload)
		if err != nil {
			return fmt.Errorf("insert message: %w", err)
		}
		n, err = q.CreateDeliveriesForMessage(ctx, msg.ID)
		if err != nil {
			return fmt.Errorf("insert deliveries: %w", err)
		}
		return nil
	})

	if err != nil {
		return sqlc.Message{}, 0, fmt.Errorf("create message: %w", err)
	}

	return msg, n, nil
}
