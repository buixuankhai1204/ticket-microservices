package usecase

import (
	"context"

	"github.com/google/uuid"
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
	if len(stale) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, &domain.RepositoryError{Err: err}
		}
		return 0, nil
	}

	// Release() is an in-memory transition check (no I/O) — run it per row, but
	// collect the seat/booking IDs and issue exactly two batched statements
	// instead of two queries per row (an N+1 pattern at reaper batch sizes).
	// ListStaleHeldReservations only ever returns 'held' rows, so Release()
	// always succeeds and always lands on the same target status, which is what
	// makes one shared UpdateSeatReservationStatusBatch call correct here.
	seatIDs := make([]uuid.UUID, 0, len(stale)*2)
	bookingIDs := make([]uuid.UUID, len(stale))
	for i := range stale {
		if rErr := stale[i].Release(); rErr != nil {
			return 0, rErr
		}
		seatIDs = append(seatIDs, stale[i].SeatIDs...)
		bookingIDs[i] = stale[i].BookingID
	}

	if err := uc.repo.ReleaseReservedSeats(ctx, tx, seatIDs); err != nil {
		return 0, err
	}
	if err := uc.repo.UpdateSeatReservationStatusBatch(ctx, tx, bookingIDs, domain.ReservationReleased); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, &domain.RepositoryError{Err: err}
	}
	return len(stale), nil
}
