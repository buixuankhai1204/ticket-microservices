//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport"
)

func TestMain(m *testing.M) {
	os.Exit(testsupport.Run(m))
}

type consumerUnderTest struct {
	topic string
	group string
	rec   *scriptedRecorder
}

func startConsumer(t *testing.T, createDLQ bool, script ...result) *consumerUnderTest {
	t.Helper()
	topic := testsupport.UniqueName("it-booking-events")
	testsupport.CreateTopic(t, topic)
	if createDLQ {
		testsupport.CreateTopic(t, topic+".dlq")
	}
	rec := &scriptedRecorder{script: append([]result(nil), script...)}
	if len(rec.script) == 0 {
		rec.script = []result{{}}
	}
	spec := kafka.BookingCancelledSpec(rec)
	spec.Group = testsupport.UniqueName("it-group")

	c := kafka.NewConsumer(kafka.Config{Brokers: testsupport.Brokers(t), Topic: topic, MaxAttempts: 3}, spec, testsupport.SilentLogger{})
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
	testsupport.Eventually(t, 60*time.Second, fmt.Sprintf("offset %d to be committed", offset), func() bool {
		return testsupport.CommittedOffset(t, c.group, c.topic) == offset
	})
}

func TestTheOffsetIsCommittedOnlyAfterTheUseCaseHasRun(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true)

	testsupport.Produce(t, c.topic, "u1", bookingCancelled().Value, testsupport.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times before the commit, want 1", c.rec.calls())
	}
	if dead := testsupport.ReadAll(t, c.topic+".dlq"); len(dead) != 0 {
		t.Fatalf("dlq has %d records, want none for a good message", len(dead))
	}
}

func TestAnEventTypeForAnotherConsumerGroupIsSkippedButStillCommitted(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true)

	testsupport.Produce(t, c.topic, "u1", bookingCancelled().Value, testsupport.EventType("BookingConfirmed"))

	c.waitForCommit(t, 1)
	if c.rec.calls() != 0 {
		t.Fatalf("use case ran for an event type this group does not own")
	}
}

func TestAPoisonMessageIsDeadLetteredWithItsOriginAndTheNextMessageStillGetsThrough(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true)

	testsupport.Produce(t, c.topic, "poison", []byte("not-json"), testsupport.EventType("BookingCancelled"))
	testsupport.Produce(t, c.topic, "good", bookingCancelled().Value, testsupport.EventType("BookingCancelled"))

	c.waitForCommit(t, 2)
	dead := testsupport.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 {
		t.Fatalf("dlq has %d records, want 1", len(dead))
	}
	m := dead[0]
	if string(m.Key) != "poison" || string(m.Value) != "not-json" {
		t.Fatalf("dlq key/value = %q/%q, want the original", m.Key, m.Value)
	}
	if !strings.HasPrefix(testsupport.Header(m, "x-dlq-reason"), "parse:") {
		t.Fatalf("x-dlq-reason = %q, want a parse: reason", testsupport.Header(m, "x-dlq-reason"))
	}
	if testsupport.Header(m, "x-dlq-source-topic") != c.topic ||
		testsupport.Header(m, "x-dlq-source-partition") != "0" ||
		testsupport.Header(m, "x-dlq-source-offset") != "0" {
		t.Fatalf("origin headers wrong: %v", m.Headers)
	}
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want only for the good message", c.rec.calls())
	}
}

func TestAPermanentFailureGoesToTheDeadLetterTopicAndIsNotRetried(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true, result{err: &domain.RepositoryError{Err: &pgconn.PgError{Code: "23505", Message: "duplicate key"}}})

	testsupport.Produce(t, c.topic, "u1", bookingCancelled().Value, testsupport.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	dead := testsupport.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 || !strings.HasPrefix(testsupport.Header(dead[0], "x-dlq-reason"), "permanent:") {
		t.Fatalf("dlq = %v, want one permanent: record", dead)
	}
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want 1 (no retries for a constraint violation)", c.rec.calls())
	}
}

func TestATransientFailureIsRetriedAndTheMessageIsCommittedOnceItSucceeds(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, true,
		result{err: &domain.RepositoryError{Err: &pgconn.PgError{Code: "40001"}}},
		result{err: &domain.RepositoryError{Err: &pgconn.PgError{Code: "40P01"}}},
		result{},
	)

	testsupport.Produce(t, c.topic, "u1", bookingCancelled().Value, testsupport.EventType("BookingCancelled"))

	c.waitForCommit(t, 1)
	if c.rec.calls() != 3 {
		t.Fatalf("use case ran %d times, want 3", c.rec.calls())
	}
	if dead := testsupport.ReadAll(t, c.topic+".dlq"); len(dead) != 0 {
		t.Fatalf("dlq has %d records, a recovered message must not be dead-lettered", len(dead))
	}
}

func TestNoMessageIsLostWhileTheDeadLetterTopicIsUnavailable(t *testing.T) {
	t.Parallel()
	c := startConsumer(t, false)

	testsupport.Produce(t, c.topic, "poison", []byte("not-json"), testsupport.EventType("BookingCancelled"))
	testsupport.Produce(t, c.topic, "good", bookingCancelled().Value, testsupport.EventType("BookingCancelled"))

	window := time.Now().Add(12 * time.Second)
	for time.Now().Before(window) {
		if got := testsupport.CommittedOffset(t, c.group, c.topic); got > 0 {
			t.Fatalf("offset %d was committed while the poison message at offset 0 could not be dead-lettered: it is lost", got)
		}
		time.Sleep(500 * time.Millisecond)
	}

	testsupport.CreateTopic(t, c.topic+".dlq")

	c.waitForCommit(t, 2)
	dead := testsupport.ReadAll(t, c.topic+".dlq")
	if len(dead) != 1 || string(dead[0].Key) != "poison" {
		t.Fatalf("dlq = %d records, want exactly the poison message", len(dead))
	}
	if c.rec.calls() != 1 {
		t.Fatalf("use case ran %d times, want only for the good message", c.rec.calls())
	}
}
