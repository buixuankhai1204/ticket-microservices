//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

func countRows(t *testing.T, ctx context.Context, query string, arg any) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(ctx, query, arg).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func TestRecordUserRegistration_IdempotentOnRedelivery(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	repo := postgres.New()
	uc := usecase.NewRecordUserRegistrationUseCase(testPool, repo)

	ev := domain.UserCreated{
		EventID:   uuid.New(),
		UserID:    uuid.New(),
		Email:     "bob@example.com",
		CreatedAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond),
	}

	already, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("first Execute: unexpected error %v", err)
	}
	if already {
		t.Fatalf("first Execute: alreadyProcessed = true, want false")
	}
	if got := countRows(t, ctx,
		`SELECT count(*) FROM user_registrations WHERE user_id = $1`, ev.UserID); got != 1 {
		t.Fatalf("after first Execute: user_registrations rows = %d, want 1", got)
	}
	if got := countRows(t, ctx,
		`SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); got != 1 {
		t.Fatalf("after first Execute: processed_events rows = %d, want 1", got)
	}

	already, err = uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("second Execute: unexpected error %v", err)
	}
	if !already {
		t.Fatalf("second Execute: alreadyProcessed = false, want true")
	}
	if got := countRows(t, ctx,
		`SELECT count(*) FROM user_registrations WHERE user_id = $1`, ev.UserID); got != 1 {
		t.Fatalf("after second Execute: user_registrations rows = %d, want 1 (no duplicate side effect)", got)
	}
	if got := countRows(t, ctx,
		`SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); got != 1 {
		t.Fatalf("after second Execute: processed_events rows = %d, want 1", got)
	}
}

func TestRecordUserRegistration_TransactionAtomicity(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	if _, err := testPool.Exec(ctx,
		`ALTER TABLE user_registrations
		 ADD CONSTRAINT no_boom CHECK (email <> 'boom@example.com')`,
	); err != nil {
		t.Fatalf("add test-only CHECK constraint: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(),
			`ALTER TABLE user_registrations DROP CONSTRAINT IF EXISTS no_boom`)
	})

	repo := postgres.New()
	uc := usecase.NewRecordUserRegistrationUseCase(testPool, repo)

	ev := domain.UserCreated{
		EventID:   uuid.New(),
		UserID:    uuid.New(),
		Email:     "boom@example.com",
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}

	_, err := uc.Execute(ctx, ev)
	if err == nil {
		t.Fatalf("Execute: expected an error from the failing insert, got nil")
	}

	if got := countRows(t, ctx,
		`SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); got != 0 {
		t.Errorf("processed_events rows = %d, want 0 (transaction should have rolled back)", got)
	}
	if got := countRows(t, ctx,
		`SELECT count(*) FROM user_registrations WHERE user_id = $1`, ev.UserID); got != 0 {
		t.Errorf("user_registrations rows = %d, want 0", got)
	}
}
