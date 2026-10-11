package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type Repository struct{}

func New() *Repository {
	return &Repository{}
}

var _ port.Repository = (*Repository)(nil)

func (r *Repository) GetEventBookingStats(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (domain.EventBookingStats, error) {
	const query = `
		SELECT
			COUNT(*) FILTER (WHERE status = 'confirmed') AS confirmed,
			COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled
		FROM booking_outcomes
		WHERE event_id = $1`

	stats := domain.EventBookingStats{EventID: eventID}
	if err := tx.QueryRow(ctx, query, eventID).Scan(&stats.Confirmed, &stats.Cancelled); err != nil {
		return domain.EventBookingStats{}, &domain.RepositoryError{Err: fmt.Errorf("query event stats: %w", err)}
	}

	return stats, nil
}

func (r *Repository) RecordUserRegistration(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, reg domain.UserRegistration) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("mark processed_events: %w", err)}
	}
	if tag.RowsAffected() == 0 {
		return true, nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO user_registrations (user_id, email, registered_at, recorded_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (user_id) DO NOTHING`,
		reg.UserID, reg.Email, reg.RegisteredAt, reg.RecordedAt,
	); err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("insert user_registration: %w", err)}
	}

	return false, nil
}

func (r *Repository) RecordUserLogin(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, login domain.UserLogin) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("mark processed_events: %w", err)}
	}
	if tag.RowsAffected() == 0 {
		return true, nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO user_logins (event_id, user_id, email, logged_in_at, recorded_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (event_id) DO NOTHING`,
		eventID, login.UserID, login.Email, login.LoggedInAt, login.RecordedAt,
	); err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("insert user_login: %w", err)}
	}

	return false, nil
}

func (r *Repository) RecordBookingOutcome(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, outcome domain.BookingOutcome) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("mark processed_events: %w", err)}
	}
	if tag.RowsAffected() == 0 {
		return true, nil
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO booking_outcomes (id, booking_id, event_id, status, occurred_at, recorded_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (booking_id) DO NOTHING`,
		outcome.ID, outcome.BookingID, outcome.EventID, outcome.Status, outcome.OccurredAt, outcome.RecordedAt,
	); err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("insert booking_outcome: %w", err)}
	}

	return false, nil
}

func (r *Repository) markProcessed(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (alreadyProcessed bool, err error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	if err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("mark processed_events: %w", err)}
	}
	return tag.RowsAffected() == 0, nil
}

func (r *Repository) RecordProductOrderPlaced(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, outcome domain.ProductOrderOutcome) (bool, error) {
	already, err := r.markProcessed(ctx, tx, eventID)
	if err != nil || already {
		return already, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO product_order_outcomes
		   (order_id, booking_id, user_id, ticketed_event_id, currency, total_minor, item_count, status, placed_at, recorded_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (order_id) DO UPDATE
		   SET placed_at = EXCLUDED.placed_at, item_count = EXCLUDED.item_count`,
		outcome.OrderID, outcome.BookingID, outcome.UserID, outcome.TicketedEventID, outcome.Currency,
		outcome.TotalMinor, outcome.ItemCount, outcome.Status, outcome.PlacedAt, outcome.RecordedAt,
	); err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("upsert product_order_outcome (placed): %w", err)}
	}
	return false, nil
}

func (r *Repository) RecordProductOrderCancelled(ctx context.Context, tx pgx.Tx, eventID uuid.UUID, outcome domain.ProductOrderOutcome) (bool, error) {
	already, err := r.markProcessed(ctx, tx, eventID)
	if err != nil || already {
		return already, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO product_order_outcomes
		   (order_id, booking_id, user_id, ticketed_event_id, currency, total_minor, item_count, status, cancelled_at, cancel_reason, recorded_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (order_id) DO UPDATE
		   SET status = EXCLUDED.status, cancelled_at = EXCLUDED.cancelled_at, cancel_reason = EXCLUDED.cancel_reason`,
		outcome.OrderID, outcome.BookingID, outcome.UserID, outcome.TicketedEventID, outcome.Currency,
		outcome.TotalMinor, outcome.ItemCount, outcome.Status, outcome.CancelledAt, outcome.CancelReason, outcome.RecordedAt,
	); err != nil {
		return false, &domain.RepositoryError{Err: fmt.Errorf("upsert product_order_outcome (cancelled): %w", err)}
	}
	return false, nil
}

func (r *Repository) GetUserRegistration(ctx context.Context, tx pgx.Tx, userID uuid.UUID) (domain.UserRegistration, error) {
	const query = `
		SELECT user_id, email, registered_at, recorded_at
		FROM user_registrations
		WHERE user_id = $1`

	var reg domain.UserRegistration
	err := tx.QueryRow(ctx, query, userID).
		Scan(&reg.UserID, &reg.Email, &reg.RegisteredAt, &reg.RecordedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.UserRegistration{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.UserRegistration{}, &domain.RepositoryError{Err: fmt.Errorf("query user_registration: %w", err)}
	}

	return reg, nil
}
