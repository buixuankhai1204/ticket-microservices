//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestReapHeldReservations_ReleasesStaleHeldOnly(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	const holdTimeoutSecs = 900
	now := time.Now().UTC()

	eventID, seatIDs := seedEventWithSeats(t, 6)
	staleSeats, freshSeats, finalizedSeats := seatIDs[0:2], seatIDs[2:4], seatIDs[4:6]

	staleBooking := seedReservation(t, eventID, staleSeats, domain.ReservationHeld, now.Add(-time.Hour))
	freshBooking := seedReservation(t, eventID, freshSeats, domain.ReservationHeld, now)
	finalizedBooking := seedReservation(t, eventID, finalizedSeats, domain.ReservationFinalized, now.Add(-2*time.Hour))

	uc := usecase.NewReapHeldReservationsUseCase(testPool, newRepo(), holdTimeoutSecs)

	reaped, err := uc.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1 (only the stale held reservation)", reaped)
	}

	if n := seatsInStatus(t, staleSeats, domain.SeatAvailable); n != len(staleSeats) {
		t.Errorf("stale reservation seats available = %d, want %d", n, len(staleSeats))
	}
	if s := reservationStatus(t, staleBooking); s != domain.ReservationReleased {
		t.Errorf("stale reservation status = %q, want %q", s, domain.ReservationReleased)
	}

	if n := seatsInStatus(t, freshSeats, domain.SeatReserved); n != len(freshSeats) {
		t.Errorf("fresh reservation seats reserved = %d, want %d (must be untouched)", n, len(freshSeats))
	}
	if s := reservationStatus(t, freshBooking); s != domain.ReservationHeld {
		t.Errorf("fresh reservation status = %q, want %q", s, domain.ReservationHeld)
	}

	if n := seatsInStatus(t, finalizedSeats, domain.SeatBooked); n != len(finalizedSeats) {
		t.Errorf("finalized reservation seats booked = %d, want %d (must be untouched)", n, len(finalizedSeats))
	}
	if s := reservationStatus(t, finalizedBooking); s != domain.ReservationFinalized {
		t.Errorf("finalized reservation status = %q, want %q", s, domain.ReservationFinalized)
	}
}
