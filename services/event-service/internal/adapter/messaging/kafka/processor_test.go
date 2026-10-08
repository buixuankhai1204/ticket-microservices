package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	segkafka "github.com/segmentio/kafka-go"
	"go.uber.org/mock/gomock"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport/mocks"
)

type result struct {
	already bool
	err     error
}

type scriptedRecorder struct {
	mu     sync.Mutex
	script []result
	seen   []domain.BookingCancelled
}

func (r *scriptedRecorder) Execute(_ context.Context, ev domain.BookingCancelled) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := len(r.seen)
	r.seen = append(r.seen, ev)
	if i >= len(r.script) {
		i = len(r.script) - 1
	}
	return r.script[i].already, r.script[i].err
}

func (r *scriptedRecorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func quick(attempts int) kafka.RetryPolicy {
	return kafka.RetryPolicy{MaxAttempts: attempts, FirstBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}
}

func bookingCancelled() segkafka.Message {
	payload := fmt.Sprintf(`{"event_id":%q,"booking_id":%q,"user_id":%q,"ticketed_event_id":%q,"seat_ids":[%q],"reason":"user_cancelled","occurred_at":"2026-03-04T08:30:00Z"}`,
		uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString())
	return segkafka.Message{
		Topic: "booking.events", Key: []byte("k"), Value: []byte(payload),
		Headers: []segkafka.Header{testsupport.EventType("BookingCancelled")},
	}
}

func processor(rec *scriptedRecorder, dlq kafka.DeadLetters, attempts int) *kafka.Processor[domain.BookingCancelled] {
	return kafka.NewProcessor(kafka.BookingCancelledSpec(rec), dlq, quick(attempts), testsupport.SilentLogger{})
}

func repoErr(err error) error {
	return &domain.RepositoryError{Err: err}
}

func TestAnEventTypeOwnedByAnotherConsumerIsIgnoredWithoutTouchingTheUseCase(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{}}}
	m := bookingCancelled()
	m.Headers = []segkafka.Header{testsupport.EventType("BookingConfirmed")}

	outcome, err := processor(rec, mocks.NewMockDeadLetters(gomock.NewController(t)), 3).Process(context.Background(), m)

	if err != nil || outcome != kafka.Ignored {
		t.Fatalf("got (%v, %v), want (ignored, nil)", outcome, err)
	}
	if rec.calls() != 0 {
		t.Fatalf("the use case ran for another consumer's event")
	}
}

func TestAMessageThatCannotBeParsedIsParkedAtOnceWithoutRetrying(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{}}}
	dlq := mocks.NewMockDeadLetters(gomock.NewController(t))
	m := bookingCancelled()
	m.Value = []byte(`{{ nope`)
	dlq.EXPECT().
		Park(gomock.Any(), gomock.Cond(func(got segkafka.Message) bool { return string(got.Value) == `{{ nope` }),
			gomock.Cond(func(reason string) bool { return strings.HasPrefix(reason, "parse:") })).
		Return(nil)

	outcome, err := processor(rec, dlq, 3).Process(context.Background(), m)

	if err != nil || outcome != kafka.Parked {
		t.Fatalf("got (%v, %v), want (parked, nil)", outcome, err)
	}
	if rec.calls() != 0 {
		t.Fatalf("an undeserializable message reached the use case")
	}
}

func TestATransientFailureIsRetriedInPlaceUntilItSucceeds(t *testing.T) {
	rec := &scriptedRecorder{script: []result{
		{err: repoErr(errors.New("pool exhausted"))},
		{err: repoErr(&pgconn.PgError{Code: "40001"})},
		{already: false},
	}}

	outcome, err := processor(rec, mocks.NewMockDeadLetters(gomock.NewController(t)), 5).Process(context.Background(), bookingCancelled())

	if err != nil || outcome != kafka.Handled {
		t.Fatalf("got (%v, %v), want (handled, nil)", outcome, err)
	}
	if rec.calls() != 3 {
		t.Fatalf("use case ran %d times, want 3", rec.calls())
	}
}

func TestRetriesThatNeverSucceedEndInTheDeadLetterTopicWithTheAttemptCount(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{err: repoErr(errors.New("connection reset"))}}}
	dlq := mocks.NewMockDeadLetters(gomock.NewController(t))
	dlq.EXPECT().
		Park(gomock.Any(), gomock.Any(), gomock.Cond(func(reason string) bool { return strings.HasPrefix(reason, "max-retries after 3 attempts") })).
		Return(nil)

	outcome, err := processor(rec, dlq, 3).Process(context.Background(), bookingCancelled())

	if err != nil || outcome != kafka.Parked {
		t.Fatalf("got (%v, %v), want (parked, nil)", outcome, err)
	}
	if rec.calls() != 3 {
		t.Fatalf("use case ran %d times, want exactly the 3 allowed attempts", rec.calls())
	}
}

func TestPostgresErrorsAreRetriedOnlyWhenTheSameInputCouldSucceedLater(t *testing.T) {
	tests := []struct {
		code      string
		wantCalls int
		wantPrefx string
	}{
		{"40001", 3, "max-retries"},
		{"40P01", 3, "max-retries"},
		{"55P03", 3, "max-retries"},
		{"53300", 3, "max-retries"},
		{"08006", 3, "max-retries"},
		{"57P01", 3, "max-retries"},
		{"23505", 1, "permanent:"},
		{"23503", 1, "permanent:"},
		{"23514", 1, "permanent:"},
		{"22P02", 1, "permanent:"},
		{"42P01", 1, "permanent:"},
		{"42703", 1, "permanent:"},
	}
	for _, tc := range tests {
		t.Run("sqlstate "+tc.code, func(t *testing.T) {
			rec := &scriptedRecorder{script: []result{{err: repoErr(&pgconn.PgError{Code: tc.code, Message: "boom"})}}}
			dlq := mocks.NewMockDeadLetters(gomock.NewController(t))
			dlq.EXPECT().
				Park(gomock.Any(), gomock.Any(), gomock.Cond(func(reason string) bool { return strings.HasPrefix(reason, tc.wantPrefx) })).
				Return(nil)

			outcome, err := processor(rec, dlq, 3).Process(context.Background(), bookingCancelled())

			if err != nil || outcome != kafka.Parked {
				t.Fatalf("got (%v, %v), want (parked, nil)", outcome, err)
			}
			if rec.calls() != tc.wantCalls {
				t.Fatalf("use case ran %d times, want %d", rec.calls(), tc.wantCalls)
			}
		})
	}
}

func TestADomainRejectionIsPermanentAndParkedWithoutRetrying(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{err: domain.ErrInvalidSeat}}}
	dlq := mocks.NewMockDeadLetters(gomock.NewController(t))
	dlq.EXPECT().
		Park(gomock.Any(), gomock.Any(), gomock.Cond(func(reason string) bool {
			return strings.HasPrefix(reason, "permanent:") && strings.Contains(reason, "invalid seat")
		})).
		Return(nil)

	outcome, err := processor(rec, dlq, 5).Process(context.Background(), bookingCancelled())

	if err != nil || outcome != kafka.Parked {
		t.Fatalf("got (%v, %v), want (parked, nil)", outcome, err)
	}
	if rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want 1", rec.calls())
	}
}

func TestADuplicateDeliveryIsHandledAndNeverParked(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{already: true}}}

	outcome, err := processor(rec, mocks.NewMockDeadLetters(gomock.NewController(t)), 3).Process(context.Background(), bookingCancelled())

	if err != nil || outcome != kafka.Handled {
		t.Fatalf("got (%v, %v), want (handled, nil)", outcome, err)
	}
}

func TestWhenTheDeadLetterWriteFailsTheMessageIsReportedUnresolvedSoItIsNeverCommitted(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{}}}
	dlq := mocks.NewMockDeadLetters(gomock.NewController(t))
	m := bookingCancelled()
	m.Value = []byte(`{{ nope`)
	dlq.EXPECT().Park(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("write to dlq: unknown topic"))

	outcome, err := processor(rec, dlq, 3).Process(context.Background(), m)

	if err == nil {
		t.Fatalf("got (%v, nil): a message that could not be dead-lettered must not look resolved", outcome)
	}
}

func TestShuttingDownDuringBackoffStopsRetryingAndLeavesTheMessageUncommitted(t *testing.T) {
	rec := &scriptedRecorder{script: []result{{err: repoErr(errors.New("connection reset"))}}}
	policy := kafka.RetryPolicy{MaxAttempts: 10, FirstBackoff: time.Hour, MaxBackoff: time.Hour}
	p := kafka.NewProcessor(kafka.BookingCancelledSpec(rec), mocks.NewMockDeadLetters(gomock.NewController(t)), policy, testsupport.SilentLogger{})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	_, err := p.Process(ctx, bookingCancelled())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want 1", rec.calls())
	}
}
