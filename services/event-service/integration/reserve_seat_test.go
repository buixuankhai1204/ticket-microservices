//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestReserveSeat_HappyPath_ReservesSeatsAndEmitsSeatReserved(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 3)
	ev := newBookingRequested(eventID, seatIDs)

	spy := newSpy()
	uc := usecase.NewReserveSeatUseCase(testPool, spy)

	already, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if already {
		t.Fatalf("alreadyProcessed = true, want false on first call")
	}

	if n := seatsInStatus(t, seatIDs, domain.SeatReserved); n != len(seatIDs) {
		t.Errorf("seats reserved = %d, want %d", n, len(seatIDs))
	}
	held := countRows(t,
		`SELECT COUNT(*) FROM seat_reservations WHERE booking_id = $1 AND event_id = $2 AND status = 'held'`,
		ev.BookingID, eventID)
	if held != 1 {
		t.Errorf("held seat_reservations rows for the booking = %d, want 1", held)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM outbox_events`); n != 0 {
		t.Errorf("outbox_events rows = %d, want 0 (WriteOutbox inserts-then-deletes)", n)
	}

	calls := spy.recorded()
	if len(calls) != 1 {
		t.Fatalf("WriteOutbox calls = %d, want 1", len(calls))
	}
	emitted, ok := calls[0].(domain.SeatReservedEvent)
	if !ok {
		t.Fatalf("emitted event = %T, want domain.SeatReservedEvent", calls[0])
	}
	if emitted.BookingID != ev.BookingID || emitted.TicketedEventID != eventID {
		t.Errorf("SeatReservedEvent ids = %s/%s, want %s/%s",
			emitted.BookingID, emitted.TicketedEventID, ev.BookingID, eventID)
	}
	if emitted.EventType() != "SeatReserved" || emitted.AggregateType() != "seat_reservation" {
		t.Errorf("routing = %s/%s, want SeatReserved/seat_reservation",
			emitted.EventType(), emitted.AggregateType())
	}
	if emitted.AggregateID() != ev.BookingID {
		t.Errorf("aggregate id = %s, want booking id %s", emitted.AggregateID(), ev.BookingID)
	}
}

func TestReserveSeat_ConcurrentContendForSameSeats_ExactlyOneWinnerPerSeat(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	const seatCount = 3
	const requests = 12

	eventID, seatIDs := seedEventWithSeats(t, seatCount)

	spy := newSpy()
	uc := usecase.NewReserveSeatUseCase(testPool, spy)

	type outcome struct {
		already bool
		err     error
	}
	outcomes := make([]outcome, requests)

	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func(i int) {
			defer wg.Done()
			ev := newBookingRequested(eventID, []uuid.UUID{seatIDs[i%seatCount]})
			already, err := uc.Execute(ctx, ev)
			outcomes[i] = outcome{already: already, err: err}
		}(i)
	}
	wg.Wait()

	for i, o := range outcomes {
		if o.err != nil {
			t.Fatalf("request %d: Execute error = %v, want nil (a lost race is a business rejection)", i, o.err)
		}
		if o.already {
			t.Fatalf("request %d: alreadyProcessed = true, want false (every event id is fresh)", i)
		}
	}

	reserved, failed := spy.countByType()
	if reserved != seatCount {
		t.Errorf("SeatReserved events = %d, want %d (one winner per seat)", reserved, seatCount)
	}
	if failed != requests-seatCount {
		t.Errorf("SeatReservationFailed events = %d, want %d", failed, requests-seatCount)
	}
	for _, ev := range spy.recorded() {
		if f, ok := ev.(domain.SeatReservationFailedEvent); ok && f.Reason != domain.ReasonSeatUnavailable {
			t.Errorf("failed event reason = %q, want %q", f.Reason, domain.ReasonSeatUnavailable)
		}
	}

	if n := seatsInStatus(t, seatIDs, domain.SeatReserved); n != seatCount {
		t.Errorf("seats in reserved status = %d, want %d", n, seatCount)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seat_reservations WHERE status = 'held'`); n != seatCount {
		t.Errorf("held seat_reservations rows = %d, want %d", n, seatCount)
	}
	heldSeats := countRows(t, `SELECT COUNT(*) FROM (SELECT unnest(seat_ids) AS sid FROM seat_reservations) t`)
	distinctHeldSeats := countRows(t, `SELECT COUNT(DISTINCT sid) FROM (SELECT unnest(seat_ids) AS sid FROM seat_reservations) t`)
	if heldSeats != seatCount || distinctHeldSeats != seatCount {
		t.Errorf("held seat ids total/distinct = %d/%d, want %d/%d (a seat was reserved twice)",
			heldSeats, distinctHeldSeats, seatCount, seatCount)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events`); n != requests {
		t.Errorf("processed_events rows = %d, want %d (winners and losers both record dedupe)", n, requests)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM outbox_events`); n != 0 {
		t.Errorf("outbox_events rows = %d, want 0", n)
	}
}

func TestReserveSeat_ReplaySameEventID_IsNoOp(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 3)
	ev := newBookingRequested(eventID, seatIDs)

	spy := newSpy()
	uc := usecase.NewReserveSeatUseCase(testPool, spy)

	already, err := uc.Execute(ctx, ev)
	if err != nil || already {
		t.Fatalf("first Execute = (%v, %v), want (false, nil)", already, err)
	}

	replayAlready, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("replay Execute error = %v, want nil", err)
	}
	if !replayAlready {
		t.Fatalf("replay alreadyProcessed = false, want true (same event id already applied)")
	}

	if n := seatsInStatus(t, seatIDs, domain.SeatReserved); n != len(seatIDs) {
		t.Errorf("seats reserved = %d, want %d (replay must not touch seats)", n, len(seatIDs))
	}
	if n := countRows(t, `SELECT COUNT(*) FROM seat_reservations`); n != 1 {
		t.Errorf("total seat_reservations rows = %d, want 1 (no duplicate hold)", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows for the event = %d, want 1", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM outbox_events`); n != 0 {
		t.Errorf("outbox_events rows = %d, want 0", n)
	}

	calls := spy.recorded()
	if len(calls) != 1 {
		t.Fatalf("WriteOutbox calls = %d, want 1 (replay emits nothing)", len(calls))
	}
	if _, ok := calls[0].(domain.SeatReservedEvent); !ok {
		t.Fatalf("first emitted event = %T, want domain.SeatReservedEvent", calls[0])
	}
}

func TestReserveSeat_MissingEventOrSeat_EmitsSeatReservationFailed(t *testing.T) {
	ctx := context.Background()

	t.Run("event_not_found", func(t *testing.T) {
		truncateAll(t)
		_, seatIDs := seedEventWithSeats(t, 2)

		spy := newSpy()
		uc := usecase.NewReserveSeatUseCase(testPool, spy)

		ev := newBookingRequested(uuid.New(), seatIDs)
		already, err := uc.Execute(ctx, ev)
		if err != nil || already {
			t.Fatalf("Execute = (%v, %v), want (false, nil)", already, err)
		}

		calls := spy.recorded()
		if len(calls) != 1 {
			t.Fatalf("WriteOutbox calls = %d, want 1", len(calls))
		}
		failed, ok := calls[0].(domain.SeatReservationFailedEvent)
		if !ok {
			t.Fatalf("emitted event = %T, want domain.SeatReservationFailedEvent", calls[0])
		}
		if failed.Reason != domain.ReasonEventNotFound {
			t.Errorf("reason = %q, want %q", failed.Reason, domain.ReasonEventNotFound)
		}
		if n := countRows(t, `SELECT COUNT(*) FROM seat_reservations`); n != 0 {
			t.Errorf("seat_reservations rows = %d, want 0", n)
		}
		if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
			t.Errorf("processed_events rows = %d, want 1", n)
		}
	})

	t.Run("seat_not_found", func(t *testing.T) {
		truncateAll(t)
		eventID, seatIDs := seedEventWithSeats(t, 2)

		spy := newSpy()
		uc := usecase.NewReserveSeatUseCase(testPool, spy)

		ev := newBookingRequested(eventID, []uuid.UUID{seatIDs[0], uuid.New()})
		already, err := uc.Execute(ctx, ev)
		if err != nil || already {
			t.Fatalf("Execute = (%v, %v), want (false, nil)", already, err)
		}

		calls := spy.recorded()
		if len(calls) != 1 {
			t.Fatalf("WriteOutbox calls = %d, want 1", len(calls))
		}
		failed, ok := calls[0].(domain.SeatReservationFailedEvent)
		if !ok {
			t.Fatalf("emitted event = %T, want domain.SeatReservationFailedEvent", calls[0])
		}
		if failed.Reason != domain.ReasonSeatNotFound {
			t.Errorf("reason = %q, want %q", failed.Reason, domain.ReasonSeatNotFound)
		}
		if n := seatsInStatus(t, seatIDs, domain.SeatAvailable); n != 2 {
			t.Errorf("seats still available = %d, want 2 (no partial reservation)", n)
		}
		if n := countRows(t, `SELECT COUNT(*) FROM seat_reservations`); n != 0 {
			t.Errorf("seat_reservations rows = %d, want 0", n)
		}
	})
}
