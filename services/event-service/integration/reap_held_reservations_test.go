//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestReapHeldReservations_ReleasesStaleHeldOnly(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	const holdTimeoutSecs = 900
	now := time.Now().UTC()

	// Two *distinct* stale bookings (not just one) so this exercises the
	// batched release/update path across more than one row — the exact shape
	// of the N+1-query bug being regression-tested here: a batching mistake
	// (e.g. releasing every seat regardless of booking, or mismatching
	// booking IDs to seat groups) would show up as cross-contamination
	// between staleBookingA and staleBookingB, not as a wrong total count.
	eventID, seatIDs := seedEventWithSeats(t, 8)
	staleSeatsA, staleSeatsB := seatIDs[0:2], seatIDs[2:4]
	freshSeats, finalizedSeats := seatIDs[4:6], seatIDs[6:8]

	staleBookingA := seedReservation(t, eventID, staleSeatsA, domain.ReservationHeld, now.Add(-time.Hour))
	staleBookingB := seedReservation(t, eventID, staleSeatsB, domain.ReservationHeld, now.Add(-2*time.Hour))
	freshBooking := seedReservation(t, eventID, freshSeats, domain.ReservationHeld, now)
	finalizedBooking := seedReservation(t, eventID, finalizedSeats, domain.ReservationFinalized, now.Add(-3*time.Hour))

	uc := usecase.NewReapHeldReservationsUseCase(testPool, newRepo(), holdTimeoutSecs)

	reaped, err := uc.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if reaped != 2 {
		t.Fatalf("reaped = %d, want 2 (both stale held reservations)", reaped)
	}

	for _, tc := range []struct {
		name    string
		booking uuid.UUID
		seats   []uuid.UUID
	}{
		{"staleBookingA", staleBookingA, staleSeatsA},
		{"staleBookingB", staleBookingB, staleSeatsB},
	} {
		if n := seatsInStatus(t, tc.seats, domain.SeatAvailable); n != len(tc.seats) {
			t.Errorf("%s seats available = %d, want %d", tc.name, n, len(tc.seats))
		}
		if s := reservationStatus(t, tc.booking); s != domain.ReservationReleased {
			t.Errorf("%s status = %q, want %q", tc.name, s, domain.ReservationReleased)
		}
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
