package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type FinalizeSeatUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewFinalizeSeatUseCase(pool *pgxpool.Pool, repo port.Repository) *FinalizeSeatUseCase {
	return &FinalizeSeatUseCase{pool: pool, repo: repo}
}

func (uc *FinalizeSeatUseCase) Execute(ctx context.Context, ev domain.BookingConfirmed) (alreadyProcessed bool, err error) {
	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	already, err := uc.repo.MarkEventProcessed(ctx, tx, ev.ID)
	if err != nil {
		return false, err
	}
	if already {
		if err := tx.Commit(ctx); err != nil {
			return false, &domain.RepositoryError{Err: err}
		}
		return true, nil
	}

	res, err := uc.repo.LockSeatReservation(ctx, tx, ev.BookingID)
	if errors.Is(err, domain.ErrNotFound) {
		return false, &domain.RepositoryError{Err: fmt.Errorf("finalize: no seat_reservations row for booking %s", ev.BookingID)}
	}
	if err != nil {
		return false, err
	}

	wasHeld := res.Status == domain.ReservationHeld
	if err := res.Finalize(); err != nil {
		return false, err
	}

	if wasHeld {
		if err := uc.repo.UpdateSeatsStatus(ctx, tx, res.SeatIDs, domain.SeatBooked); err != nil {
			return false, err
		}
		if err := uc.repo.UpdateSeatReservationStatus(ctx, tx, ev.BookingID, res.Status); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return false, nil
}
