//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

func TestRecordUserRegistration_ProjectsRowThenAbsorbsRedelivery(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	uc := usecase.NewRecordUserRegistrationUseCase(testPool, newRepo())
	ev := domain.UserCreated{
		EventID:   uuid.New(),
		UserID:    uuid.New(),
		Email:     "bob@example.com",
		CreatedAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond),
	}

	already, err := uc.Execute(ctx, ev)
	if err != nil || already {
		t.Fatalf("first Execute = (%v, %v), want (false, nil)", already, err)
	}
	reg, ok := queryUserRegistration(t, ctx, ev.UserID)
	if !ok {
		t.Fatalf("no user_registrations row projected for %s", ev.UserID)
	}
	if reg.Email != ev.Email || !reg.RegisteredAt.Equal(ev.CreatedAt) || reg.RecordedAt.IsZero() {
		t.Errorf("projected row = %+v, want email %q registered_at %s", reg, ev.Email, ev.CreatedAt)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}

	replayAlready, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("replay Execute error = %v, want nil", err)
	}
	if !replayAlready {
		t.Fatalf("replay alreadyProcessed = false, want true")
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM user_registrations WHERE user_id = $1`, ev.UserID); n != 1 {
		t.Fatalf("user_registrations rows after replay = %d, want 1", n)
	}

	fresh := ev
	fresh.EventID = uuid.New()
	freshAlready, err := uc.Execute(ctx, fresh)
	if err != nil {
		t.Fatalf("fresh-event-id Execute error = %v, want nil", err)
	}
	if freshAlready {
		t.Fatalf("fresh-event-id alreadyProcessed = true, want false (new event id is unseen)")
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM user_registrations WHERE user_id = $1`, ev.UserID); n != 1 {
		t.Errorf("user_registrations rows = %d, want 1 (UNIQUE(user_id) is the second backstop)", n)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events`); n != 2 {
		t.Errorf("processed_events rows = %d, want 2", n)
	}
}

func TestRecordUserLogin_ProjectsRowThenAbsorbsRedelivery(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	uc := usecase.NewRecordUserLoginUseCase(testPool, newRepo())
	ev := domain.UserLoggedIn{
		EventID:    uuid.New(),
		UserID:     uuid.New(),
		Email:      "carol@example.com",
		LoggedInAt: time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Microsecond),
	}

	already, err := uc.Execute(ctx, ev)
	if err != nil || already {
		t.Fatalf("first Execute = (%v, %v), want (false, nil)", already, err)
	}
	login, ok := queryUserLogin(t, ctx, ev.EventID)
	if !ok {
		t.Fatalf("no user_logins row projected for event %s", ev.EventID)
	}
	if login.UserID != ev.UserID || login.Email != ev.Email || !login.LoggedInAt.Equal(ev.LoggedInAt) {
		t.Errorf("projected row = %+v, want user %s email %q logged_in_at %s", login, ev.UserID, ev.Email, ev.LoggedInAt)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}

	replayAlready, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("replay Execute error = %v, want nil", err)
	}
	if !replayAlready {
		t.Fatalf("replay alreadyProcessed = false, want true")
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM user_logins WHERE event_id = $1`, ev.EventID); n != 1 {
		t.Errorf("user_logins rows after replay = %d, want 1 (event_id PK is the backstop)", n)
	}
}

func TestRecordBookingConfirmed_ProjectsOutcomeThenAbsorbsRedelivery(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	uc := usecase.NewRecordBookingConfirmedUseCase(testPool, newRepo())
	ev := domain.BookingConfirmed{
		EventID:         uuid.New(),
		BookingID:       uuid.New(),
		UserID:          uuid.New(),
		TicketedEventID: uuid.New(),
		OccurredAt:      time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond),
	}

	already, err := uc.Execute(ctx, ev)
	if err != nil || already {
		t.Fatalf("first Execute = (%v, %v), want (false, nil)", already, err)
	}
	outcome, ok := queryBookingOutcome(t, ctx, ev.BookingID)
	if !ok {
		t.Fatalf("no booking_outcomes row projected for %s", ev.BookingID)
	}
	if outcome.Status != domain.OutcomeConfirmed || outcome.EventID != ev.TicketedEventID || !outcome.OccurredAt.Equal(ev.OccurredAt) {
		t.Errorf("outcome = %+v, want status confirmed event_id %s occurred_at %s", outcome, ev.TicketedEventID, ev.OccurredAt)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}

	replayAlready, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("replay Execute error = %v, want nil", err)
	}
	if !replayAlready {
		t.Fatalf("replay alreadyProcessed = false, want true")
	}

	fresh := ev
	fresh.EventID = uuid.New()
	freshAlready, err := uc.Execute(ctx, fresh)
	if err != nil {
		t.Fatalf("fresh-event-id Execute error = %v, want nil", err)
	}
	if freshAlready {
		t.Fatalf("fresh-event-id alreadyProcessed = true, want false")
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM booking_outcomes WHERE booking_id = $1`, ev.BookingID); n != 1 {
		t.Errorf("booking_outcomes rows = %d, want 1 (UNIQUE(booking_id) is the second backstop)", n)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events`); n != 2 {
		t.Errorf("processed_events rows = %d, want 2", n)
	}
}

func TestRecordBookingCancelled_ProjectsOutcomeThenAbsorbsRedelivery(t *testing.T) {
	truncateAll(t)
	ctx := context.Background()

	uc := usecase.NewRecordBookingCancelledUseCase(testPool, newRepo())
	ev := domain.BookingCancelled{
		EventID:         uuid.New(),
		BookingID:       uuid.New(),
		UserID:          uuid.New(),
		TicketedEventID: uuid.New(),
		Reason:          "seat_unavailable",
		OccurredAt:      time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Microsecond),
	}

	already, err := uc.Execute(ctx, ev)
	if err != nil || already {
		t.Fatalf("first Execute = (%v, %v), want (false, nil)", already, err)
	}
	outcome, ok := queryBookingOutcome(t, ctx, ev.BookingID)
	if !ok {
		t.Fatalf("no booking_outcomes row projected for %s", ev.BookingID)
	}
	if outcome.Status != domain.OutcomeCancelled || outcome.EventID != ev.TicketedEventID {
		t.Errorf("outcome = %+v, want status cancelled event_id %s", outcome, ev.TicketedEventID)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events WHERE event_id = $1`, ev.EventID); n != 1 {
		t.Fatalf("processed_events rows = %d, want 1", n)
	}

	replayAlready, err := uc.Execute(ctx, ev)
	if err != nil {
		t.Fatalf("replay Execute error = %v, want nil", err)
	}
	if !replayAlready {
		t.Fatalf("replay alreadyProcessed = false, want true")
	}

	fresh := ev
	fresh.EventID = uuid.New()
	freshAlready, err := uc.Execute(ctx, fresh)
	if err != nil {
		t.Fatalf("fresh-event-id Execute error = %v, want nil", err)
	}
	if freshAlready {
		t.Fatalf("fresh-event-id alreadyProcessed = true, want false")
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM booking_outcomes WHERE booking_id = $1`, ev.BookingID); n != 1 {
		t.Errorf("booking_outcomes rows = %d, want 1 (UNIQUE(booking_id) is the second backstop)", n)
	}
	if n := countRows(t, ctx, `SELECT count(*) FROM processed_events`); n != 2 {
		t.Errorf("processed_events rows = %d, want 2", n)
	}
}
