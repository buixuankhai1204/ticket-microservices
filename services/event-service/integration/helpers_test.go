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

func newRepo() *postgres.Repository {
	return postgres.New()
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

func (s *outboxSpy) countByType() (reserved, failed int) {
	for _, ev := range s.recorded() {
		switch ev.(type) {
		case domain.SeatReservedEvent:
			reserved++
		case domain.SeatReservationFailedEvent:
			failed++
		}
	}
	return reserved, failed
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
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

var seatStatusForReservation = map[string]string{
	domain.ReservationHeld:      domain.SeatReserved,
	domain.ReservationFinalized: domain.SeatBooked,
	domain.ReservationReleased:  domain.SeatAvailable,
}

func seedReservation(t *testing.T, eventID uuid.UUID, seatIDs []uuid.UUID, status string, createdAt time.Time) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	bookingID := uuid.New()

	seatStatus, ok := seatStatusForReservation[status]
	if !ok {
		t.Fatalf("seedReservation: unknown reservation status %q", status)
	}

	if _, err := testPool.Exec(ctx,
		`UPDATE seats SET status = $1 WHERE id = ANY($2)`, seatStatus, seatIDs,
	); err != nil {
		t.Fatalf("seedReservation set seats %s: %v", seatStatus, err)
	}
	if _, err := testPool.Exec(ctx,
		`INSERT INTO seat_reservations (booking_id, event_id, seat_ids, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $5)`,
		bookingID, eventID, seatIDs, status, createdAt,
	); err != nil {
		t.Fatalf("seedReservation insert: %v", err)
	}
	return bookingID
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

func newBookingConfirmed(bookingID, ticketedEventID uuid.UUID, seatIDs []uuid.UUID) domain.BookingConfirmed {
	return domain.BookingConfirmed{
		ID:              uuid.New(),
		BookingID:       bookingID,
		UserID:          uuid.New(),
		TicketedEventID: ticketedEventID,
		SeatIDs:         seatIDs,
		OccurredAt:      time.Now().UTC(),
	}
}

func newBookingCancelled(bookingID, ticketedEventID uuid.UUID, seatIDs []uuid.UUID) domain.BookingCancelled {
	return domain.BookingCancelled{
		ID:              uuid.New(),
		BookingID:       bookingID,
		UserID:          uuid.New(),
		TicketedEventID: ticketedEventID,
		SeatIDs:         seatIDs,
		Reason:          "seat_unavailable",
		OccurredAt:      time.Now().UTC(),
	}
}

func seatsInStatus(t *testing.T, seatIDs []uuid.UUID, status string) int {
	t.Helper()
	return countRows(t,
		`SELECT COUNT(*) FROM seats WHERE id = ANY($1) AND status = $2`, seatIDs, status)
}

func mustPagination(t *testing.T, limit, offset int) domain.Pagination {
	t.Helper()
	p, err := domain.NewPagination(limit, offset)
	if err != nil {
		t.Fatalf("NewPagination(%d, %d): %v", limit, offset, err)
	}
	return p
}

func createEventViaUseCase(t *testing.T, name string, startsAt, endsAt time.Time, sections []domain.SectionSpec) domain.Event {
	t.Helper()
	ev, _, err := usecase.NewCreateNewEventUseCase(testPool, newRepo()).Execute(context.Background(), usecase.CreateNewEventInput{
		Name:     name,
		Venue:    "Test Arena",
		StartsAt: startsAt,
		EndsAt:   endsAt,
		Layout:   domain.LayoutSpec{Sections: sections},
	})
	if err != nil {
		t.Fatalf("create event %q: %v", name, err)
	}
	return ev
}

func reservationStatus(t *testing.T, bookingID uuid.UUID) string {
	t.Helper()
	var status string
	err := testPool.QueryRow(context.Background(),
		`SELECT status FROM seat_reservations WHERE booking_id = $1`, bookingID).Scan(&status)
	if err != nil {
		t.Fatalf("read seat_reservations status for %s: %v", bookingID, err)
	}
	return status
}
