//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	segkafka "github.com/segmentio/kafka-go"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
)

type probeEvent struct {
	ID string `json:"id"`
}

type outcome struct {
	already bool
	err     error
}

type scriptedRecorder struct {
	mu     sync.Mutex
	script []outcome
	seen   []probeEvent
}

func (r *scriptedRecorder) Execute(_ context.Context, ev probeEvent) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := len(r.seen)
	r.seen = append(r.seen, ev)
	if len(r.script) == 0 {
		return false, nil
	}
	if i >= len(r.script) {
		i = len(r.script) - 1
	}
	return r.script[i].already, r.script[i].err
}

func (r *scriptedRecorder) events() []probeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]probeEvent(nil), r.seen...)
}

type silentLogger struct{}

func (silentLogger) Info(string, ...any)         {}
func (silentLogger) Warn(string, ...any)         {}
func (silentLogger) Error(string, ...any)        {}
func (l silentLogger) With(...any) logger.Logger { return l }

func brokers() []string {
	if v := os.Getenv("KAFKA_TEST_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9094"}
}

func adminClient() *segkafka.Client {
	return &segkafka.Client{Addr: segkafka.TCP(brokers()...), Timeout: 10 * time.Second}
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

func createTopic(t *testing.T, name string) {
	t.Helper()
	resp, err := adminClient().CreateTopics(context.Background(), &segkafka.CreateTopicsRequest{
		Topics: []segkafka.TopicConfig{{Topic: name, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	if e := resp.Errors[name]; e != nil {
		t.Fatalf("create topic %s: %v", name, e)
	}
	eventually(t, 15*time.Second, "topic "+name+" to have a leader", func() bool {
		md, err := adminClient().Metadata(context.Background(), &segkafka.MetadataRequest{Topics: []string{name}})
		if err != nil || len(md.Topics) != 1 || md.Topics[0].Error != nil || len(md.Topics[0].Partitions) != 1 {
			return false
		}
		return md.Topics[0].Partitions[0].Error == nil
	})
}

func newTopics(t *testing.T, withDLQ bool) (topic, group string) {
	t.Helper()
	id := uuid.NewString()[:8]
	topic, group = "it-"+id, "it-group-"+id
	createTopic(t, topic)
	if withDLQ {
		createTopic(t, topic+".dlq")
	}
	return topic, group
}

func produce(t *testing.T, topic, key, value string, headers ...segkafka.Header) {
	t.Helper()
	w := &segkafka.Writer{
		Addr:         segkafka.TCP(brokers()...),
		Topic:        topic,
		Balancer:     &segkafka.Hash{},
		RequiredAcks: segkafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()
	err := w.WriteMessages(context.Background(), segkafka.Message{Key: []byte(key), Value: []byte(value), Headers: headers})
	if err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}

func committedOffset(t *testing.T, group, topic string) int64 {
	t.Helper()
	resp, err := adminClient().OffsetFetch(context.Background(), &segkafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: {0}},
	})
	if err != nil || len(resp.Topics[topic]) != 1 {
		return -1
	}
	return resp.Topics[topic][0].CommittedOffset
}

func endOffset(t *testing.T, topic string) int64 {
	t.Helper()
	resp, err := adminClient().ListOffsets(context.Background(), &segkafka.ListOffsetsRequest{
		Topics: map[string][]segkafka.OffsetRequest{topic: {segkafka.LastOffsetOf(0)}},
	})
	if err != nil || len(resp.Topics[topic]) != 1 {
		t.Fatalf("list offsets for %s: %v", topic, err)
	}
	return resp.Topics[topic][0].LastOffset
}

func readAll(t *testing.T, topic string) []segkafka.Message {
	t.Helper()
	n := endOffset(t, topic)
	r := segkafka.NewReader(segkafka.ReaderConfig{Brokers: brokers(), Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer r.Close()
	if err := r.SetOffset(segkafka.FirstOffset); err != nil {
		t.Fatalf("set offset: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out []segkafka.Message
	for int64(len(out)) < n {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s: %v", topic, err)
		}
		out = append(out, m)
	}
	return out
}

func header(m segkafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func startConsumer(t *testing.T, topic, group string, maxAttempts int, rec Recorder[probeEvent]) {
	t.Helper()
	spec := EventSpec[probeEvent]{
		Group:      group,
		EventType:  "Probe",
		Component:  "probe_consumer",
		SuccessMsg: "probe processed",
		Parse: func(b []byte) (probeEvent, error) {
			var ev probeEvent
			if err := json.Unmarshal(b, &ev); err != nil {
				return probeEvent{}, err
			}
			if ev.ID == "" {
				return probeEvent{}, errors.New("missing id")
			}
			return ev, nil
		},
		LogFields: func(ev probeEvent) []any { return []any{"id", ev.ID} },
		Record:    rec,
	}
	c := NewConsumer(Config{Brokers: brokers(), Topic: topic, MaxAttempts: maxAttempts}, spec, silentLogger{})
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
}

func probeType(v string) segkafka.Header {
	return segkafka.Header{Key: "event_type", Value: []byte(v)}
}

func TestConsumerHandlesAMatchingEventAndCommitsItsOffset(t *testing.T) {
	t.Parallel()
	topic, group := newTopics(t, true)
	rec := &scriptedRecorder{}
	produce(t, topic, "k1", `{"id":"e1"}`, probeType("Probe"))

	startConsumer(t, topic, group, 3, rec)

	eventually(t, 45*time.Second, "offset 1 to be committed", func() bool { return committedOffset(t, group, topic) == 1 })
	if got := rec.events(); len(got) != 1 || got[0].ID != "e1" {
		t.Fatalf("handled events = %+v, want exactly e1", got)
	}
}

func TestConsumerSkipsAndCommitsAnEventTypeOwnedByAnotherGroup(t *testing.T) {
	t.Parallel()
	topic, group := newTopics(t, true)
	rec := &scriptedRecorder{}
	produce(t, topic, "k2", `{"id":"e2"}`, probeType("SomethingElse"))

	startConsumer(t, topic, group, 3, rec)

	eventually(t, 45*time.Second, "offset 1 to be committed", func() bool { return committedOffset(t, group, topic) == 1 })
	if got := rec.events(); len(got) != 0 {
		t.Fatalf("handler was called for a foreign event type: %+v", got)
	}
}

func TestConsumerDeadLettersAPoisonMessageWithItsOriginAndCommits(t *testing.T) {
	t.Parallel()
	topic, group := newTopics(t, true)
	rec := &scriptedRecorder{}
	produce(t, topic, "k3", "not-json", probeType("Probe"))

	startConsumer(t, topic, group, 3, rec)

	eventually(t, 45*time.Second, "offset 1 to be committed", func() bool { return committedOffset(t, group, topic) == 1 })
	dead := readAll(t, topic+".dlq")
	if len(dead) != 1 {
		t.Fatalf("dlq has %d records, want 1", len(dead))
	}
	m := dead[0]
	if string(m.Key) != "k3" || string(m.Value) != "not-json" {
		t.Fatalf("dlq key/value = %q/%q, want the original k3/not-json", m.Key, m.Value)
	}
	if !strings.HasPrefix(header(m, "x-dlq-reason"), "parse:") {
		t.Fatalf("x-dlq-reason = %q, want a parse: reason", header(m, "x-dlq-reason"))
	}
	if header(m, "x-dlq-source-topic") != topic || header(m, "x-dlq-source-partition") != "0" || header(m, "x-dlq-source-offset") != "0" {
		t.Fatalf("dlq origin headers = %q/%q/%q, want %s/0/0",
			header(m, "x-dlq-source-topic"), header(m, "x-dlq-source-partition"), header(m, "x-dlq-source-offset"), topic)
	}
	if len(rec.events()) != 0 {
		t.Fatalf("handler must not see an undeserializable message")
	}
}

func TestConsumerClassifiesHandlerFailures(t *testing.T) {
	t.Parallel()
	transient := &domain.RepositoryError{Err: errors.New("pool acquire timeout")}
	permanent := &domain.RepositoryError{Err: &pgconn.PgError{Code: "23505"}}

	tests := []struct {
		name       string
		script     []outcome
		wantCalls  int
		wantDead   int
		wantReason string
	}{
		{name: "permanent goes straight to the dlq", script: []outcome{{err: permanent}}, wantCalls: 1, wantDead: 1, wantReason: "permanent:"},
		{name: "transient then success retries without a dlq", script: []outcome{{err: transient}, {err: transient}, {}}, wantCalls: 3, wantDead: 0},
		{name: "transient forever dead-letters after max attempts", script: []outcome{{err: transient}}, wantCalls: 3, wantDead: 1, wantReason: "max-retries after 3 attempts"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic, group := newTopics(t, true)
			rec := &scriptedRecorder{script: tc.script}
			produce(t, topic, "k4", `{"id":"e4"}`, probeType("Probe"))

			startConsumer(t, topic, group, 3, rec)

			eventually(t, 60*time.Second, "offset 1 to be committed", func() bool { return committedOffset(t, group, topic) == 1 })
			if got := len(rec.events()); got != tc.wantCalls {
				t.Fatalf("handler calls = %d, want %d", got, tc.wantCalls)
			}
			dead := readAll(t, topic+".dlq")
			if len(dead) != tc.wantDead {
				t.Fatalf("dlq records = %d, want %d", len(dead), tc.wantDead)
			}
			if tc.wantDead == 1 && !strings.HasPrefix(header(dead[0], "x-dlq-reason"), tc.wantReason) {
				t.Fatalf("x-dlq-reason = %q, want prefix %q", header(dead[0], "x-dlq-reason"), tc.wantReason)
			}
		})
	}
}

func TestConsumerNeverLosesAMessageWhileTheDLQIsUnavailable(t *testing.T) {
	t.Parallel()
	topic, group := newTopics(t, false)
	rec := &scriptedRecorder{}
	produce(t, topic, "poison", "not-json", probeType("Probe"))
	produce(t, topic, "good", `{"id":"good"}`, probeType("Probe"))

	startConsumer(t, topic, group, 3, rec)

	window := time.Now().Add(25 * time.Second)
	for time.Now().Before(window) {
		if got := committedOffset(t, group, topic); got > 0 {
			t.Fatalf("offset %s was committed while the poison message at offset 0 could not be dead-lettered: it is lost", strconv.FormatInt(got, 10))
		}
		time.Sleep(500 * time.Millisecond)
	}

	createTopic(t, topic+".dlq")

	eventually(t, 90*time.Second, "both messages to be committed once the dlq exists", func() bool { return committedOffset(t, group, topic) == 2 })
	dead := readAll(t, topic+".dlq")
	if len(dead) != 1 || string(dead[0].Key) != "poison" {
		t.Fatalf("dlq = %d records, want exactly the poison message", len(dead))
	}
	if got := rec.events(); len(got) != 1 || got[0].ID != "good" {
		t.Fatalf("handled events = %+v, want exactly the good message", got)
	}
}
