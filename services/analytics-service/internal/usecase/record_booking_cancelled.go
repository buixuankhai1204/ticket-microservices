package usecase

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type RecordBookingCancelledUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewRecordBookingCancelledUseCase(pool *pgxpool.Pool, repo port.Repository) *RecordBookingCancelledUseCase {
	return &RecordBookingCancelledUseCase{pool: pool, repo: repo}
}

func (uc *RecordBookingCancelledUseCase) Execute(ctx context.Context, ev domain.BookingCancelled) (alreadyProcessed bool, err error) {
	outcome, err := domain.NewBookingOutcome(ev.BookingID, ev.TicketedEventID, domain.OutcomeCancelled, ev.OccurredAt)
	if err != nil {
		return false, err
	}

	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	already, err := uc.repo.RecordBookingOutcome(ctx, tx, ev.EventID, *outcome)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return already, nil
}
