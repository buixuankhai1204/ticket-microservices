package usecase

import (
	"context"
	"errors"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type ReleaseSeatUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewReleaseSeatUseCase(db port.Transactor, repo port.Repository) *ReleaseSeatUseCase {
	return &ReleaseSeatUseCase{db: db, repo: repo}
}

func (uc *ReleaseSeatUseCase) Execute(ctx context.Context, ev domain.BookingCancelled) (alreadyProcessed bool, err error) {
	tx, err := uc.db.Begin(ctx)
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
		if err := tx.Commit(ctx); err != nil {
			return false, &domain.RepositoryError{Err: err}
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}

	wasHeld := res.Status == domain.ReservationHeld
	if err := res.Release(); err != nil {
		return false, err
	}

	if wasHeld {
		if err := uc.repo.UpdateSeatsStatus(ctx, tx, res.SeatIDs, domain.SeatAvailable); err != nil {
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
