//go:build integration

package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/tests/common"
)

var repo = postgres.New()

func inTx(t *testing.T, pool *pgxpool.Pool, fn func(tx pgx.Tx)) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func registration(t *testing.T, userID uuid.UUID, email string) domain.UserRegistration {
	t.Helper()
	reg, err := domain.NewUserRegistration(userID, email, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	return *reg
}

func outcome(t *testing.T, bookingID, eventID uuid.UUID, status string) domain.BookingOutcome {
	t.Helper()
	o, err := domain.NewBookingOutcome(bookingID, eventID, status, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return *o
}

func TestARegistrationIsStoredAndReadBackThroughTheSameTransactionFreeQuery(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	userID := uuid.New()
	want := registration(t, userID, "ada@example.com")

	inTx(t, pool, func(tx pgx.Tx) {
		already, err := repo.RecordUserRegistration(context.Background(), tx, uuid.New(), want)
		if err != nil || already {
			t.Fatalf("got (%v, %v), want (false, nil)", already, err)
		}
	})

	inTx(t, pool, func(tx pgx.Tx) {
		got, err := repo.GetUserRegistration(context.Background(), tx, userID)
		if err != nil {
			t.Fatalf("GetUserRegistration: %v", err)
		}
		if got.UserID != userID || got.Email != "ada@example.com" || !got.RegisteredAt.Equal(want.RegisteredAt) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
}

func TestReplayingAnEventIsReportedAndStoresNothingNew(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	eventID := uuid.New()
	reg := registration(t, uuid.New(), "ada@example.com")

	var first, second bool
	inTx(t, pool, func(tx pgx.Tx) {
		first, _ = repo.RecordUserRegistration(context.Background(), tx, eventID, reg)
	})
	inTx(t, pool, func(tx pgx.Tx) {
		var err error
		second, err = repo.RecordUserRegistration(context.Background(), tx, eventID, reg)
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
	})

	if first || !second {
		t.Fatalf("alreadyProcessed = (%v, %v), want (false, true)", first, second)
	}
	if n := count(t, pool, "user_registrations"); n != 1 {
		t.Fatalf("user_registrations has %d rows, want 1", n)
	}
	if n := count(t, pool, "processed_events"); n != 1 {
		t.Fatalf("processed_events has %d rows, want 1", n)
	}
}

func TestARolledBackWriteDoesNotMarkTheEventAsProcessed(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	ctx := context.Background()
	eventID := uuid.New()
	reg := registration(t, uuid.New(), "ada@example.com")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.RecordUserRegistration(ctx, tx, eventID, reg); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	inTx(t, pool, func(tx pgx.Tx) {
		already, err := repo.RecordUserRegistration(ctx, tx, eventID, reg)
		if err != nil || already {
			t.Fatalf("retry after rollback = (%v, %v), want it applied for real this time", already, err)
		}
	})
	if n := count(t, pool, "user_registrations"); n != 1 {
		t.Fatalf("user_registrations has %d rows, want 1", n)
	}
}

func TestEventStatsCountEachStatusOnlyForThatEvent(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	eventID, otherEvent := uuid.New(), uuid.New()
	record := func(eventID uuid.UUID, status string) {
		inTx(t, pool, func(tx pgx.Tx) {
			if _, err := repo.RecordBookingOutcome(context.Background(), tx, uuid.New(), outcome(t, uuid.New(), eventID, status)); err != nil {
				t.Fatalf("record outcome: %v", err)
			}
		})
	}
	record(eventID, domain.OutcomeConfirmed)
	record(eventID, domain.OutcomeConfirmed)
	record(eventID, domain.OutcomeCancelled)
	record(otherEvent, domain.OutcomeConfirmed)

	inTx(t, pool, func(tx pgx.Tx) {
		stats, err := repo.GetEventBookingStats(context.Background(), tx, eventID)
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.Confirmed != 2 || stats.Cancelled != 1 || stats.EventID != eventID {
			t.Fatalf("stats = %+v, want 2 confirmed 1 cancelled", stats)
		}
		none, err := repo.GetEventBookingStats(context.Background(), tx, uuid.New())
		if err != nil || none.Confirmed != 0 || none.Cancelled != 0 {
			t.Fatalf("stats for an unknown event = (%+v, %v), want zeros", none, err)
		}
	})
}

func TestOnlyTheFirstOutcomeForABookingCounts(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)
	bookingID, eventID := uuid.New(), uuid.New()

	for _, status := range []string{domain.OutcomeConfirmed, domain.OutcomeCancelled} {
		inTx(t, pool, func(tx pgx.Tx) {
			if _, err := repo.RecordBookingOutcome(context.Background(), tx, uuid.New(), outcome(t, bookingID, eventID, status)); err != nil {
				t.Fatalf("record outcome: %v", err)
			}
		})
	}

	inTx(t, pool, func(tx pgx.Tx) {
		stats, err := repo.GetEventBookingStats(context.Background(), tx, eventID)
		if err != nil || stats.Confirmed != 1 || stats.Cancelled != 0 {
			t.Fatalf("stats = (%+v, %v), want the booking counted once, as confirmed", stats, err)
		}
	})
}

func TestAnUnknownUserIsNotFound(t *testing.T) {
	t.Parallel()
	pool := common.NewDatabase(t)

	inTx(t, pool, func(tx pgx.Tx) {
		_, err := repo.GetUserRegistration(context.Background(), tx, uuid.New())
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}
