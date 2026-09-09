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

func TestReleaseSeat_HappyPath_HeldBecomesAvailableAndReleased(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	bookingID := seedReservation(t, eventID, seatIDs, domain.ReservationHeld, time.Now().UTC())
	ev := newBookingCancelled(bookingID, eventID, seatIDs)

	uc := usecase.NewReleaseSeatUseCase(testPool, newRepo())

	already, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if already {
		t.Fatalf("alreadyProcessed = true, want false on first call")
	}

	if n := seatsInStatus(t, seatIDs, domain.SeatAvailable); n != len(seatIDs) {
		t.Errorf("seats available = %d, want %d", n, len(seatIDs))
	}
	if s := reservationStatus(t, bookingID); s != domain.ReservationReleased {
		t.Errorf("reservation status = %q, want %q", s, domain.ReservationReleased)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1", n)
	}
}

func TestReleaseSeat_ReplaySameEventID_IsIdempotent(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	bookingID := seedReservation(t, eventID, seatIDs, domain.ReservationHeld, time.Now().UTC())
	ev := newBookingCancelled(bookingID, eventID, seatIDs)

	uc := usecase.NewReleaseSeatUseCase(testPool, newRepo())

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

	if n := seatsInStatus(t, seatIDs, domain.SeatAvailable); n != len(seatIDs) {
		t.Errorf("seats available = %d, want %d (replay must not change seats)", n, len(seatIDs))
	}
	if s := reservationStatus(t, bookingID); s != domain.ReservationReleased {
		t.Errorf("reservation status = %q, want %q", s, domain.ReservationReleased)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1 (no duplicate marker)", n)
	}
}

func TestReleaseSeat_MissingReservationRow_IsIdempotentNoOp(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	ev := newBookingCancelled(uuid.New(), eventID, seatIDs)

	uc := usecase.NewReleaseSeatUseCase(testPool, newRepo())

	already, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("Execute error = %v, want nil (no held row is a legitimate no-op, saga doc 5.1)", err)
	}
	if already {
		t.Fatalf("alreadyProcessed = true, want false")
	}

	if n := countRows(t, `SELECT COUNT(*) FROM seat_reservations`); n != 0 {
		t.Errorf("seat_reservations rows = %d, want 0", n)
	}
	if n := seatsInStatus(t, seatIDs, domain.SeatAvailable); n != len(seatIDs) {
		t.Errorf("seats available = %d, want %d (untouched)", n, len(seatIDs))
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1 (no-op path still commits the dedupe marker)", n)
	}
}

func TestReleaseSeat_FinalizedReservation_ReturnsPermanentError(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 2)
	bookingID := seedReservation(t, eventID, seatIDs, domain.ReservationFinalized, time.Now().UTC())
	ev := newBookingCancelled(bookingID, eventID, seatIDs)

	uc := usecase.NewReleaseSeatUseCase(testPool, newRepo())

	already, err := uc.Execute(ctx, ev)
	if already {
		t.Fatalf("alreadyProcessed = true, want false")
	}
	if !errors.Is(err, domain.ErrReservationNotHeld) {
		t.Fatalf("error = %v, want domain.ErrReservationNotHeld (permanent)", err)
	}
	if s := reservationStatus(t, bookingID); s != domain.ReservationFinalized {
		t.Errorf("reservation status = %q, want unchanged %q", s, domain.ReservationFinalized)
	}
	if n := seatsInStatus(t, seatIDs, domain.SeatBooked); n != len(seatIDs) {
		t.Errorf("seats booked = %d, want unchanged %d", n, len(seatIDs))
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 0 {
		t.Errorf("processed_events rows = %d, want 0 (transaction rolled back)", n)
	}
}
