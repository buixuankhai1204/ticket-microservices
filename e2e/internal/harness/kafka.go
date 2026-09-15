//go:build e2e

package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

// KafkaReachable makes a short-lived direct TCP dial to the first configured
// broker. This repo's docker-compose.yml deliberately advertises Kafka
// in-network only (kafka:9092, no host port mapping -- see its comment: "use
// `docker compose exec kafka ...` to poke it from the host"), so on an
// unmodified stack this returns false and callers should skip the check/test
// cleanly (never fail) rather than report a false saga defect.
func KafkaReachable(ctx context.Context) bool {
	brokers := KafkaBrokers()
	if len(brokers) == 0 {
		return false
	}
	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", brokers[0])
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// NewWriter is a general-purpose producer for a topic, keyed consistently by
// message key (kafka.Hash -- a CRC32 hash of the key, so repeated writes with
// the same key land on the same partition within this writer's own view of
// the topic; it does not need to match Debezium's own Java-default
// partitioner, since correctness here never depends on landing on a specific
// partition).
func NewWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(KafkaBrokers()...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
}

// WriteToPartition writes msgs directly to one specific partition of topic,
// bypassing any balancer entirely (kafka.Conn, not kafka.Writer) so a test
// can guarantee two messages -- e.g. a poison message and the legitimate one
// that follows it -- land on the same partition, which a balancer's hash
// cannot promise across two independent calls with different keys.
func WriteToPartition(ctx context.Context, topic string, partition int, msgs ...kafka.Message) error {
	brokers := KafkaBrokers()
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, partition)
	if err != nil {
		return fmt.Errorf("dial leader for %s/%d: %w", topic, partition, err)
	}
	defer conn.Close()
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	if _, err := conn.WriteMessages(msgs...); err != nil {
		return fmt.Errorf("write to %s/%d: %w", topic, partition, err)
	}
	return nil
}

// HeaderValue returns the first header value matching key, or "".
func HeaderValue(m kafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

type wireEnvelope struct {
	EventID   string `json:"event_id"`
	BookingID string `json:"booking_id"`
}

// FindMessage scans topic from its earliest offset (a fresh, throwaway
// consumer group -- reading the whole topic never disturbs any real consumer
// group's committed offsets) until match returns true or timeout elapses.
// Used to locate a real, already-produced saga event (e.g. to snoop the exact
// bytes of a BookingConfirmed message for the idempotency test) or to wait
// for a message this test itself just produced (e.g. its own DLQ entry).
func FindMessage(ctx context.Context, topic string, timeout time.Duration, match func(kafka.Message) bool) (kafka.Message, error) {
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     KafkaBrokers(),
		GroupID:     "e2e-scan-" + uuid.NewString(),
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     250 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(readCtx)
		if err != nil {
			return kafka.Message{}, fmt.Errorf("scan %s for a matching message: %w", topic, err)
		}
		if match(m) {
			return m, nil
		}
	}
}

// MatchEventTypeAndBooking returns a FindMessage matcher for a specific
// event_type header plus a booking_id in the JSON payload -- every wire event
// in this saga carries both (docs/sagas/seat-reservation.md §3).
func MatchEventTypeAndBooking(eventType, bookingID string) func(kafka.Message) bool {
	return func(m kafka.Message) bool {
		if HeaderValue(m, "event_type") != eventType {
			return false
		}
		var env wireEnvelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			return false
		}
		return env.BookingID == bookingID
	}
}

// MatchKey returns a FindMessage matcher on the raw message key.
func MatchKey(key string) func(kafka.Message) bool {
	return func(m kafka.Message) bool { return string(m.Key) == key }
}

// DLQWatch tails a topic from "now" (a fresh consumer group positioned at
// kafka.LastOffset) so a test can assert nothing new arrives on it during a
// bounded window -- the "DLQs stay empty" standing assertion for the happy
// path, the oversell/compensation path, and the idempotency test. If the
// broker is unreachable from the host, AssertEmpty logs and skips the check
// instead of failing the test, matching TestMain's "skip, don't fail" policy
// for infra this environment doesn't expose (see KafkaReachable).
type DLQWatch struct {
	reader *kafka.Reader
	topic  string
}

func NewDLQWatch(ctx context.Context, topic string) *DLQWatch {
	if !KafkaReachable(ctx) {
		return &DLQWatch{topic: topic}
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     KafkaBrokers(),
		GroupID:     "e2e-dlq-watch-" + uuid.NewString(),
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     200 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})
	return &DLQWatch{reader: reader, topic: topic}
}

func (w *DLQWatch) AssertEmpty(t *testing.T, ctx context.Context, wait time.Duration) {
	t.Helper()
	if w.reader == nil {
		t.Logf("kafka broker unreachable from host at %v; skipping empty-DLQ check on %s (docker-compose.yml's kafka service has no host-mapped listener -- see e2e module notes)", KafkaBrokers(), w.topic)
		return
	}
	defer w.reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	m, err := w.reader.ReadMessage(readCtx)
	if err != nil {
		return // timed out with nothing arriving: exactly what we want.
	}
	t.Errorf("unexpected message on %s: partition=%d offset=%d key=%q headers=%+v",
		w.topic, m.Partition, m.Offset, string(m.Key), m.Headers)
}
