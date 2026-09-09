//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestFinalizeSeat_HappyPath_HeldBecomesBookedAndFinalized(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	bookingID := seedReservation(t, eventID, seatIDs, domain.ReservationHeld, time.Now().UTC())
	ev := newBookingConfirmed(bookingID, eventID, seatIDs)

	uc := usecase.NewFinalizeSeatUseCase(testPool, newRepo())

	already, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if already {
		t.Fatalf("alreadyProcessed = true, want false on first call")
	}

	if n := seatsInStatus(t, seatIDs, domain.SeatBooked); n != len(seatIDs) {
		t.Errorf("seats booked = %d, want %d", n, len(seatIDs))
	}
	if s := reservationStatus(t, bookingID); s != domain.ReservationFinalized {
		t.Errorf("reservation status = %q, want %q", s, domain.ReservationFinalized)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1", n)
	}
}

func TestFinalizeSeat_ReplaySameEventID_IsNoOp(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	bookingID := seedReservation(t, eventID, seatIDs, domain.ReservationHeld, time.Now().UTC())
	ev := newBookingConfirmed(bookingID, eventID, seatIDs)

	uc := usecase.NewFinalizeSeatUseCase(testPool, newRepo())

	if already, err := uc.Execute(ctx, ev); err != nil || already {
		t.Fatalf("first Execute = (%v, %v), want (false, nil)", already, err)
	}

	replayAlready, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("replay Execute error = %v, want nil", err)
	}
	if !replayAlready {
		t.Fatalf("replay alreadyProcessed = false, want true")
	}

	if n := seatsInStatus(t, seatIDs, domain.SeatBooked); n != len(seatIDs) {
		t.Errorf("seats booked = %d, want %d (replay must not change seats)", n, len(seatIDs))
	}
	if s := reservationStatus(t, bookingID); s != domain.ReservationFinalized {
		t.Errorf("reservation status = %q, want %q", s, domain.ReservationFinalized)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1 (no duplicate marker)", n)
	}
}

func TestFinalizeSeat_MissingReservationRow_ReturnsTransientError(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	ev := newBookingConfirmed(uuid.New(), eventID, seatIDs)

	uc := usecase.NewFinalizeSeatUseCase(testPool, newRepo())

	already, err := uc.Execute(ctx, ev)
	if already {
		t.Fatalf("alreadyProcessed = true, want false")
	}
	if err == nil {
		t.Fatalf("Execute error = nil, want a transient RepositoryError (saga doc 5.6)")
	}
	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) {
		t.Errorf("error = %v, want it to wrap *domain.RepositoryError (retryable classification)", err)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 0 {
		t.Errorf("processed_events rows = %d, want 0 (transaction rolled back)", n)
	}
}

func TestFinalizeSeat_ReleasedReservation_ReturnsPermanentError(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	bookingID := seedReservation(t, eventID, seatIDs, domain.ReservationReleased, time.Now().UTC())
	ev := newBookingConfirmed(bookingID, eventID, seatIDs)

	uc := usecase.NewFinalizeSeatUseCase(testPool, newRepo())

	already, err := uc.Execute(ctx, ev)
	if already {
		t.Fatalf("alreadyProcessed = true, want false")
	}
	if !errors.Is(err, domain.ErrReservationNotHeld) {
		t.Fatalf("error = %v, want domain.ErrReservationNotHeld (permanent, dead-lettered)", err)
	}
	var repoErr *domain.RepositoryError
	if errors.As(err, &repoErr) {
		t.Errorf("error wraps *domain.RepositoryError, want a permanent (non-retryable) sentinel")
	}
	if s := reservationStatus(t, bookingID); s != domain.ReservationReleased {
		t.Errorf("reservation status = %q, want unchanged %q", s, domain.ReservationReleased)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 0 {
		t.Errorf("processed_events rows = %d, want 0 (transaction rolled back)", n)
	}
}
