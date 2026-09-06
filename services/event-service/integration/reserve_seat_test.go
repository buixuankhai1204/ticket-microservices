//go:build integration

package integration

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

type outboxSpy struct {
	port.Repository
	mu    sync.Mutex
	calls []domain.OutboxEvent
}

var _ port.Repository = (*outboxSpy)(nil)

func newSpy() *outboxSpy {
	return &outboxSpy{Repository: postgres.New()}
}

func (s *outboxSpy) WriteOutbox(ctx context.Context, tx pgx.Tx, ev domain.OutboxEvent) error {
	s.mu.Lock()
	s.calls = append(s.calls, ev)
	s.mu.Unlock()
	return s.Repository.WriteOutbox(ctx, tx, ev)
}

func (s *outboxSpy) recorded() []domain.OutboxEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.OutboxEvent, len(s.calls))
	copy(out, s.calls)
	return out
}

func seedEventWithSeats(t *testing.T, seatCount int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	repo := postgres.New()

	ev, err := domain.NewEvent("Reserve Saga Show", "", "Test Arena",
		time.Now().UTC().Add(24*time.Hour), time.Now().UTC().Add(27*time.Hour))
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	seats := make([]domain.Seat, 0, seatCount)
	ids := make([]uuid.UUID, 0, seatCount)
	for i := 0; i < seatCount; i++ {
		s, err := domain.NewSeat(ev.ID, "A", "1", strconv.Itoa(i+1), 1000)
		if err != nil {
			t.Fatalf("NewSeat %d: %v", i, err)
		}
		seats = append(seats, *s)
		ids = append(ids, s.ID)
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin seed: %v", err)
	}
	if err := repo.CreateEventWithSeats(ctx, tx, *ev, seats); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("CreateEventWithSeats: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit seed: %v", err)
	}
	return ev.ID, ids
}

func newBookingRequested(ticketedEventID uuid.UUID, seatIDs []uuid.UUID) domain.BookingRequested {
	return domain.BookingRequested{
		ID:              uuid.New(),
		BookingID:       uuid.New(),
		UserID:          uuid.New(),
		TicketedEventID: ticketedEventID,
		SeatIDs:         seatIDs,
		RequestedAt:     time.Now().UTC(),
	}
}

func setSeatStatus(t *testing.T, seatID uuid.UUID, status string) {
	t.Helper()
	tag, err := testPool.Exec(context.Background(),
		`UPDATE seats SET status = $1 WHERE id = $2`, status, seatID)
	if err != nil {
		t.Fatalf("set seat %s status=%s: %v", seatID, status, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("set seat %s: %d rows affected, want 1", seatID, tag.RowsAffected())
	}
}

func seatsInStatus(t *testing.T, seatIDs []uuid.UUID, status string) int {
	t.Helper()
	return countRows(t,
		`SELECT COUNT(*) FROM seats WHERE id = ANY($1) AND status = $2`, seatIDs, status)
}

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
}

func TestReserveSeat_OneRequestedSeatUnavailable_FailsAllOrNothing(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	eventID, seatIDs := seedEventWithSeats(t, 3)
	setSeatStatus(t, seatIDs[1], domain.SeatReserved)
	ev := newBookingRequested(eventID, seatIDs)

	spy := newSpy()
	uc := usecase.NewReserveSeatUseCase(testPool, spy)

	already, err := uc.Execute(ctx, ev)
	if err != nil || already {
		t.Fatalf("Execute = (%v, %v), want (false, nil) — a business rejection is not an error", already, err)
	}

	calls := spy.recorded()
	if len(calls) != 1 {
		t.Fatalf("WriteOutbox calls = %d, want 1", len(calls))
	}
	failed, ok := calls[0].(domain.SeatReservationFailedEvent)
	if !ok {
		t.Fatalf("emitted event = %T, want domain.SeatReservationFailedEvent", calls[0])
	}
	if failed.Reason != domain.ReasonSeatUnavailable {
		t.Errorf("reason = %q, want %q", failed.Reason, domain.ReasonSeatUnavailable)
	}

	if n := countRows(t, `SELECT COUNT(*) FROM seat_reservations`); n != 0 {
		t.Errorf("seat_reservations rows = %d, want 0 (nothing held)", n)
	}
	if n := seatsInStatus(t, []uuid.UUID{seatIDs[0], seatIDs[2]}, domain.SeatAvailable); n != 2 {
		t.Errorf("other requested seats still available = %d, want 2 (no partial reservation)", n)
	}
	if n := seatsInStatus(t, seatIDs, domain.SeatReserved); n != 1 {
		t.Errorf("reserved seats among the request = %d, want 1 (only the pre-existing hold)", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, ev.ID); n != 1 {
		t.Errorf("processed_events rows = %d, want 1 (dedupe row still recorded)", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM outbox_events`); n != 0 {
		t.Errorf("outbox_events rows = %d, want 0", n)
	}
}
