package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type ReserveSeatUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewReserveSeatUseCase(pool *pgxpool.Pool, repo port.Repository) *ReserveSeatUseCase {
	return &ReserveSeatUseCase{pool: pool, repo: repo}
}

func (uc *ReserveSeatUseCase) Execute(ctx context.Context, ev domain.BookingRequested) (alreadyProcessed bool, err error) {
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

	outboxEvent, err := uc.reserveOrFail(ctx, tx, ev, time.Now().UTC())
	if err != nil {
		return false, err
	}

	if err := uc.repo.WriteOutbox(ctx, tx, outboxEvent); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return false, nil
}

func (uc *ReserveSeatUseCase) reserveOrFail(ctx context.Context, tx pgx.Tx, ev domain.BookingRequested, now time.Time) (domain.OutboxEvent, error) {
	seats, err := uc.repo.LockSeatsForReservation(ctx, tx, ev.TicketedEventID, ev.SeatIDs)
	if errors.Is(err, domain.ErrNotFound) {
		return domain.NewSeatReservationFailedEvent(ev.BookingID, ev.TicketedEventID, ev.SeatIDs, domain.ReasonEventNotFound, now), nil
	}
	if err != nil {
		return nil, err
	}

	if len(seats) != len(ev.SeatIDs) {
		return domain.NewSeatReservationFailedEvent(ev.BookingID, ev.TicketedEventID, ev.SeatIDs, domain.ReasonSeatNotFound, now), nil
	}

	for i := range seats {
		if rErr := seats[i].Reserve(); rErr != nil {
			return domain.NewSeatReservationFailedEvent(ev.BookingID, ev.TicketedEventID, ev.SeatIDs, domain.ReasonSeatUnavailable, now), nil
		}
	}

	if err := uc.repo.UpdateSeatsStatus(ctx, tx, ev.SeatIDs, domain.SeatReserved); err != nil {
		return nil, err
	}
	if err := uc.repo.CreateSeatReservation(ctx, tx, ev.BookingID, ev.TicketedEventID, ev.SeatIDs); err != nil {
		return nil, err
	}

	return domain.NewSeatReservedEvent(ev.BookingID, ev.TicketedEventID, ev.SeatIDs, now), nil
}
