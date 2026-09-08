package usecase

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type ReapHeldReservationsUseCase struct {
	pool            *pgxpool.Pool
	repo            port.Repository
	holdTimeoutSecs int
}

func NewReapHeldReservationsUseCase(pool *pgxpool.Pool, repo port.Repository, holdTimeoutSecs int) *ReapHeldReservationsUseCase {
	return &ReapHeldReservationsUseCase{pool: pool, repo: repo, holdTimeoutSecs: holdTimeoutSecs}
}

func (uc *ReapHeldReservationsUseCase) Execute(ctx context.Context) (reaped int, err error) {
	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return 0, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	stale, err := uc.repo.ListStaleHeldReservations(ctx, tx, uc.holdTimeoutSecs)
	if err != nil {
		return 0, err
	}

	for i := range stale {
		res := stale[i]
		if rErr := res.Release(); rErr != nil {
			return 0, rErr
		}
		if err := uc.repo.ReleaseReservedSeats(ctx, tx, res.SeatIDs); err != nil {
			return 0, err
		}
		if err := uc.repo.UpdateSeatReservationStatus(ctx, tx, res.BookingID, res.Status); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, &domain.RepositoryError{Err: err}
	}
	return len(stale), nil
}
