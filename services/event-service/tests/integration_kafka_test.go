//go:build integration

package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/tests/common"
)

type recorderResult struct {
	already bool
	err     error
}

type scriptedRecorder struct {
	mu     sync.Mutex
	script []recorderResult
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

func cancelledPayload() []byte {
	return []byte(fmt.Sprintf(
		`{"event_id":%q,"booking_id":%q,"user_id":%q,"ticketed_event_id":%q,"seat_ids":[%q],"reason":"user_cancelled","occurred_at":"2026-03-04T08:30:00Z"}`,
		uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()))
}

type consumerUnderTest struct {
	topic string
	group string
	rec   *scriptedRecorder
}

func startConsumer(t *testing.T, createDLQ bool, maxAttempts int, script ...recorderResult) *consumerUnderTest {
	t.Helper()
	topic := common.UniqueName("it-booking-events")
	common.CreateTopic(t, topic)
	if createDLQ {
		common.CreateTopic(t, topic+".dlq")
	}
	rec := &scriptedRecorder{script: append([]recorderResult(nil), script...)}
	if len(rec.script) == 0 {
		rec.script = []recorderResult{{}}
	}
	spec := kafka.BookingCancelledSpec(rec)
	spec.Group = common.UniqueName("it-group")

	c := kafka.NewConsumer(kafka.Config{Brokers: common.Brokers(t), Topic: topic, MaxAttempts: maxAttempts}, spec, common.SilentLogger{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = c.Close()
	})
	return &consumerUnderTest{topic: topic, group: spec.Group, rec: rec}
}

func (c *consumerUnderTest) waitForCommit(t *testing.T, offset int64) {
	t.Helper()
	common.Eventually(t, 60*time.Second, fmt.Sprintf("offset %d to be committed", offset), func() bool {
		return common.CommittedOffset(t, c.group, c.topic) == offset
	})
}

func repoFailure(code string) error {
	return &domain.RepositoryError{Err: &pgconn.PgError{Code: code, Message: "boom"}}
}

func TestTheOffsetIsCommittedOnlyAfterTheUseCaseHasRun(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, 3)

	common.Produce(t, c.topic, "b1", cancelledPayload(), common.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times before the commit, want 1", c.rec.calls())
	}
	if dead := common.ReadAll(t, c.topic+".dlq"); len(dead) != 0 {
		t.Fatalf("dlq has %d records, want none for a good message", len(dead))
	}
}

func TestAnEventTypeForAnotherConsumerGroupIsSkippedButStillCommitted(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, 3)

	common.Produce(t, c.topic, "b1", cancelledPayload(), common.EventType("BookingConfirmed"))

	c.waitForCommit(t, 1)
	if c.rec.calls() != 0 {
		t.Fatalf("use case ran for an event type this group does not own")
	}
}

func TestAPoisonMessageIsDeadLetteredWithItsOriginAndTheNextMessageStillGetsThrough(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, 3)

	common.Produce(t, c.topic, "poison", []byte("not-json"), common.EventType("BookingCancelled"))
	common.Produce(t, c.topic, "good", cancelledPayload(), common.EventType("BookingCancelled"))

	c.waitForCommit(t, 2)
	dead := common.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 {
		t.Fatalf("dlq has %d records, want 1", len(dead))
	}
	m := dead[0]
	if string(m.Key) != "poison" || string(m.Value) != "not-json" {
		t.Fatalf("dlq key/value = %q/%q, want the original", m.Key, m.Value)
	}
	if !strings.HasPrefix(common.Header(m, "x-dlq-reason"), "parse:") {
		t.Fatalf("x-dlq-reason = %q, want a parse: reason", common.Header(m, "x-dlq-reason"))
	}
	if common.Header(m, "x-dlq-source-topic") != c.topic || common.Header(m, "x-dlq-source-partition") != "0" || common.Header(m, "x-dlq-source-offset") != "0" {
		t.Fatalf("origin headers wrong: %v", m.Headers)
	}
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want only for the good message", c.rec.calls())
	}
}

func TestAPermanentFailureGoesToTheDeadLetterTopicAndIsNotRetried(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, 3, recorderResult{err: repoFailure("23505")})

	common.Produce(t, c.topic, "b1", cancelledPayload(), common.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	dead := common.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 || !strings.HasPrefix(common.Header(dead[0], "x-dlq-reason"), "permanent:") {
		t.Fatalf("dlq = %v, want one permanent: record", dead)
	}
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want 1 (no retries for a constraint violation)", c.rec.calls())
	}
}

func TestATransientFailureIsRetriedAndTheMessageIsCommittedOnceItSucceeds(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, 5, recorderResult{err: repoFailure("40001")}, recorderResult{err: repoFailure("40P01")}, recorderResult{})

	common.Produce(t, c.topic, "b1", cancelledPayload(), common.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	if c.rec.calls() != 3 {
		t.Fatalf("use case ran %d times, want 3", c.rec.calls())
	}
	if dead := common.ReadAll(t, c.topic+".dlq"); len(dead) != 0 {
		t.Fatalf("dlq has %d records, a recovered message must not be dead-lettered", len(dead))
	}
}

func TestRetriesThatNeverSucceedEndInTheDeadLetterTopicWithTheAttemptCount(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, 3, recorderResult{err: repoFailure("40001")})

	common.Produce(t, c.topic, "b1", cancelledPayload(), common.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	dead := common.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 || !strings.HasPrefix(common.Header(dead[0], "x-dlq-reason"), "max-retries after 3 attempts") {
		t.Fatalf("dlq = %v, want one max-retries after 3 attempts record", dead)
	}
	if c.rec.calls() != 3 {
		t.Fatalf("use case ran %d times, want exactly the 3 allowed attempts", c.rec.calls())
	}
}

func TestNoMessageIsLostWhileTheDeadLetterTopicIsUnavailable(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, false, 3)

	common.Produce(t, c.topic, "poison", []byte("not-json"), common.EventType("BookingCancelled"))
	common.Produce(t, c.topic, "good", cancelledPayload(), common.EventType("BookingCancelled"))

	window := time.Now().Add(12 * time.Second)
	for time.Now().Before(window) {
		if got := common.CommittedOffset(t, c.group, c.topic); got > 0 {
			t.Fatalf("offset %d was committed while the poison message at offset 0 could not be dead-lettered: it is lost", got)
		}
		time.Sleep(500 * time.Millisecond)
	}

	common.CreateTopic(t, c.topic+".dlq")

	c.waitForCommit(t, 2)
	dead := common.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 || string(dead[0].Key) != "poison" {
		t.Fatalf("dlq = %d records, want exactly the poison message", len(dead))
	}
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want only for the good message", c.rec.calls())
	}
}
