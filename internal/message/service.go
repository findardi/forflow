// Package message ingests events and fans them out to endpoints.
package message

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"forflow/internal/store/sqlc"
)

// ErrKeyReused means an idempotency key came back with a different payload.
// Returning the first message would silently drop the second one.
var ErrKeyReused = errors.New("idempotency key reused with a different payload")

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
// A non-empty key makes the call idempotent: a repeat returns the first message
// and its delivery count without fanning out again.
func (s *Service) Create(ctx context.Context, payload []byte, key string) (sqlc.Message, int64, error) {
	var (
		msg sqlc.Message
		n   int64
	)

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := sqlc.New(tx)
		dbKey := pgtype.Text{String: key, Valid: key != ""}

		var err error
		msg, err = q.CreateMessage(ctx, sqlc.CreateMessageParams{Payload: payload, IdempotencyKey: dbKey})
		if errors.Is(err, pgx.ErrNoRows) {
			// Each statement gets a fresh snapshot, so the winner's row is visible here.
			msg, err = q.GetMessageByIdempotencyKey(ctx, dbKey)
			if err != nil {
				return fmt.Errorf("read message by key: %w", err)
			}
			if !bytes.Equal(msg.Payload, payload) {
				return ErrKeyReused
			}
			deliveries, err := q.GetDeliveriesByMessageID(ctx, msg.ID)
			if err != nil {
				return fmt.Errorf("read deliveries: %w", err)
			}
			n = int64(len(deliveries))
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert message: %w", err)
		}
		n, err = q.CreateDeliveriesForMessage(ctx, msg.ID)
		if err != nil {
			return fmt.Errorf("insert deliveries: %w", err)
		}
		// No deliveries, nothing to wake for.
		if n > 0 {
			if err := q.NotifyDeliveries(ctx); err != nil {
				return fmt.Errorf("notify deliveries: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return sqlc.Message{}, 0, fmt.Errorf("create message: %w", err)
	}

	return msg, n, nil
}
