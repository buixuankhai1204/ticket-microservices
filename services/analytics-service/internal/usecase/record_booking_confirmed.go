package usecase

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type RecordBookingConfirmedUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewRecordBookingConfirmedUseCase(pool *pgxpool.Pool, repo port.Repository) *RecordBookingConfirmedUseCase {
	return &RecordBookingConfirmedUseCase{pool: pool, repo: repo}
}

func (uc *RecordBookingConfirmedUseCase) Execute(ctx context.Context, ev domain.BookingConfirmed) (alreadyProcessed bool, err error) {
	outcome, err := domain.NewBookingOutcome(ev.BookingID, ev.TicketedEventID, domain.OutcomeConfirmed, ev.OccurredAt)
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
