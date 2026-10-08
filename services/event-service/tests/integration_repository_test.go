//go:build integration

package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/tests/common"
)

var (
	repo = postgres.New()
	bg   = context.Background()
)

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := pool.Begin(bg)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(bg)
	fn(tx)
	if err := tx.Commit(bg); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func seedEvent(t *testing.T, pool *pgxpool.Pool, name string, startsAt, endsAt time.Time, rows, perRow int) (domain.Event, []domain.Seat) {
	t.Helper()
	event, seats, err := domain.NewEventWithSeats(name, "", "Hall", startsAt, endsAt, domain.LayoutSpec{
		Sections: []domain.SectionSpec{{Name: "A", Rows: rows, SeatsPerRow: perRow, PriceMinor: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	inTx(t, pool, func(tx pgx.Tx) {
		if err := repo.CreateEventWithSeats(bg, tx, *event, seats); err != nil {
			t.Fatalf("seed event: %v", err)
		}
	})
	return *event, seats
}

func upcoming() (time.Time, time.Time) {
	start := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	return start, start.Add(2 * time.Hour)
}

func ids(seats []domain.Seat) []uuid.UUID {
	out := make([]uuid.UUID, len(seats))
	for i, s := range seats {
		out[i] = s.ID
	}
	return out
}

func statusOf(t *testing.T, pool *pgxpool.Pool, seatID uuid.UUID) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(bg, `SELECT status FROM seats WHERE id = $1`, seatID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestAnEventAndItsSeatMapAreStoredTogetherAndReadBackInPositionOrder(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	start, end := upcoming()
	event, seats := seedEvent(t, pool, "Gala", start, end, 2, 3)

	inTx(t, pool, func(tx pgx.Tx) {
		got, err := repo.GetEvent(bg, tx, event.ID)
		if err != nil || got.Name != "Gala" || !got.StartsAt.Equal(start) {
			t.Fatalf("GetEvent = (%+v, %v)", got, err)
		}

		page, total, err := repo.ListSeatsForEvent(bg, tx, event.ID, domain.Pagination{Limit: 4, Offset: 0})
		if err != nil || total != len(seats) || len(page) != 4 {
			t.Fatalf("first page = (%d rows, total %d, %v), want 4 of %d", len(page), total, err, len(seats))
		}
		rest, _, err := repo.ListSeatsForEvent(bg, tx, event.ID, domain.Pagination{Limit: 4, Offset: 4})
		if err != nil || len(rest) != 2 {
			t.Fatalf("second page = (%d rows, %v), want 2", len(rest), err)
		}
		all := append(page, rest...)
		for i := 1; i < len(all); i++ {
			prev, cur := all[i-1], all[i]
			if prev.Row+prev.Number > cur.Row+cur.Number {
				t.Fatalf("seats out of position order: %s/%s before %s/%s", prev.Row, prev.Number, cur.Row, cur.Number)
			}
		}
	})
}

func TestAnUnknownEventIsNotFoundForEveryRead(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	unknown := uuid.New()

	inTx(t, pool, func(tx pgx.Tx) {
		if _, err := repo.GetEvent(bg, tx, unknown); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("GetEvent err = %v", err)
		}
		if _, _, err := repo.ListSeatsForEvent(bg, tx, unknown, domain.Pagination{Limit: 10}); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("ListSeatsForEvent err = %v", err)
		}
		if _, err := repo.LockSeatsForReservation(bg, tx, unknown, []uuid.UUID{uuid.New()}); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("LockSeatsForReservation err = %v", err)
		}
		if _, err := repo.LockSeatReservation(bg, tx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("LockSeatReservation err = %v", err)
		}
	})
}

func TestSeatsThatCannotBeStoredTakeTheEventDownWithThem(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	start, end := upcoming()
	event, seats, err := domain.NewEventWithSeats("Gala", "", "Hall", start, end, domain.LayoutSpec{
		Sections: []domain.SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 2, PriceMinor: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	seats[1].Row, seats[1].Number = seats[0].Row, seats[0].Number

	tx, err := pool.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	err = repo.CreateEventWithSeats(bg, tx, *event, seats)
	_ = tx.Rollback(bg)

	var repoErr *domain.RepositoryError
	if !errors.As(err, &repoErr) || pgCode(err) != "23505" {
		t.Fatalf("err = %v (sqlstate %q), want a RepositoryError carrying the unique violation so the consumer can classify it", err, pgCode(err))
	}
	var n int
	if err := pool.QueryRow(bg, `SELECT count(*) FROM events`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d events survived a failed seat insert (err %v), want 0", n, err)
	}
}

func TestEventsAreListedNewestFirstAndTheUpcomingFilterDropsFinishedOnes(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	past := time.Now().UTC().Add(-48 * time.Hour)
	start, end := upcoming()
	seedEvent(t, pool, "Finished", past, past.Add(time.Hour), 1, 1)
	seedEvent(t, pool, "Older", start, end, 1, 1)
	time.Sleep(5 * time.Millisecond)
	seedEvent(t, pool, "Newer", start, end, 1, 1)

	inTx(t, pool, func(tx pgx.Tx) {
		all, total, err := repo.ListEvents(bg, tx, domain.EventFilter{}, domain.Pagination{Limit: 10})
		if err != nil || total != 3 || all[0].Name != "Newer" || all[2].Name != "Finished" {
			t.Fatalf("all = (%v, total %d, %v), want newest first", names(all), total, err)
		}
		live, total, err := repo.ListEvents(bg, tx, domain.EventFilter{UpcomingOnly: true}, domain.Pagination{Limit: 10})
		if err != nil || total != 2 || len(live) != 2 {
			t.Fatalf("upcoming = (%v, total %d, %v), want the 2 unfinished events", names(live), total, err)
		}
		paged, total, err := repo.ListEvents(bg, tx, domain.EventFilter{}, domain.Pagination{Limit: 1, Offset: 1})
		if err != nil || total != 3 || len(paged) != 1 || paged[0].Name != "Older" {
			t.Fatalf("page = (%v, total %d, %v), want the middle event with the total unaffected by the page", names(paged), total, err)
		}
	})
}

func names(events []domain.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Name
	}
	return out
}

func TestLockingSeatsReturnsOnlyThoseOfThatEventAndMakesRivalsWait(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	start, end := upcoming()
	event, seats := seedEvent(t, pool, "Gala", start, end, 1, 3)
	_, otherSeats := seedEvent(t, pool, "Other", start, end, 1, 1)
	wanted := ids(seats[:2])

	first, err := pool.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback(bg)
	locked, err := repo.LockSeatsForReservation(bg, first, event.ID, append(wanted, otherSeats[0].ID))
	if err != nil || len(locked) != 2 {
		t.Fatalf("locked %d seats (err %v), want only the 2 that belong to the event", len(locked), err)
	}

	rival, err := pool.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	defer rival.Rollback(bg)
	if _, err := rival.Exec(bg, `SET LOCAL lock_timeout = '300ms'`); err != nil {
		t.Fatal(err)
	}
	_, err = repo.LockSeatsForReservation(bg, rival, event.ID, wanted)
	if pgCode(err) != "55P03" {
		t.Fatalf("rival err = %v (sqlstate %q), want it blocked on the row lock (55P03): two bookings must not reserve one seat", err, pgCode(err))
	}
}

func TestOnlyReservedSeatsAreReleasedAndBookedOnesStayBooked(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	start, end := upcoming()
	_, seats := seedEvent(t, pool, "Gala", start, end, 1, 3)
	reserved, booked, free := seats[0].ID, seats[1].ID, seats[2].ID

	inTx(t, pool, func(tx pgx.Tx) {
		if err := repo.UpdateSeatsStatus(bg, tx, []uuid.UUID{reserved}, domain.SeatReserved); err != nil {
			t.Fatal(err)
		}
		if err := repo.UpdateSeatsStatus(bg, tx, []uuid.UUID{booked}, domain.SeatBooked); err != nil {
			t.Fatal(err)
		}
		if err := repo.ReleaseReservedSeats(bg, tx, []uuid.UUID{reserved, booked, free}); err != nil {
			t.Fatal(err)
		}
	})

	if got := []string{statusOf(t, pool, reserved), statusOf(t, pool, booked), statusOf(t, pool, free)}; got[0] != "available" || got[1] != "booked" || got[2] != "available" {
		t.Fatalf("statuses = %v, want [available booked available]", got)
	}
}

func TestOnlyHeldReservationsOlderThanTheTimeoutAreStaleAndLockedOnesAreSkipped(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	oldHeld, freshHeld, oldFinalized, oldLocked := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inTx(t, pool, func(tx pgx.Tx) {
		for _, id := range []uuid.UUID{oldHeld, freshHeld, oldFinalized, oldLocked} {
			if err := repo.CreateSeatReservation(bg, tx, id, uuid.New(), []uuid.UUID{uuid.New()}); err != nil {
				t.Fatal(err)
			}
		}
		if err := repo.UpdateSeatReservationStatus(bg, tx, oldFinalized, domain.ReservationFinalized); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := pool.Exec(bg, `UPDATE seat_reservations SET created_at = now() - interval '2 hours' WHERE booking_id = ANY($1)`,
		[]uuid.UUID{oldHeld, oldFinalized, oldLocked}); err != nil {
		t.Fatal(err)
	}

	holder, err := pool.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(bg)
	if _, err := repo.LockSeatReservation(bg, holder, oldLocked); err != nil {
		t.Fatal(err)
	}

	inTx(t, pool, func(tx pgx.Tx) {
		stale, err := repo.ListStaleHeldReservations(bg, tx, 3600)
		if err != nil || len(stale) != 1 || stale[0].BookingID != oldHeld {
			t.Fatalf("stale = (%+v, %v), want only the old held reservation nobody is working on", stale, err)
		}
	})
}

func TestThePublishedEventLeavesTheOutboxEmptyYetReachesTheLogWithItsPayload(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	tap := common.TapOutbox(t, pool)
	bookingID, eventID, seatID := uuid.New(), uuid.New(), uuid.New()
	reserved := domain.NewSeatReservedEvent(bookingID, eventID, []uuid.UUID{seatID}, time.Now().UTC())

	inTx(t, pool, func(tx pgx.Tx) {
		if err := repo.WriteOutbox(bg, tx, reserved); err != nil {
			t.Fatal(err)
		}
	})

	var rows int
	if err := pool.QueryRow(bg, `SELECT count(*) FROM outbox_events`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("outbox_events holds %d rows (err %v), want the row deleted in the same transaction", rows, err)
	}
	published := tap.Events()
	if len(published) != 1 {
		t.Fatalf("published %d events, want 1", len(published))
	}
	p := published[0]
	if p.ID != reserved.ID || p.AggregateID != bookingID || p.AggregateType != "seat_reservation" || p.EventType != "SeatReserved" {
		t.Fatalf("published = %+v", p)
	}
	if p.Payload["booking_id"] != bookingID.String() || p.Payload["ticketed_event_id"] != eventID.String() {
		t.Fatalf("payload = %v", p.Payload)
	}
}
