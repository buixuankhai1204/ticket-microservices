package testsupport

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	segkafka "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	kafkaOnce    sync.Once
	kafkaBrokers []string
	kafkaErr     error
)

func Brokers(t testing.TB) []string {
	t.Helper()
	kafkaOnce.Do(func() {
		if v := os.Getenv("TEST_KAFKA_BROKERS"); v != "" {
			kafkaBrokers = strings.Split(v, ",")
			return
		}
		ctx := context.Background()
		container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
			testcontainers.WithEnv(map[string]string{"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false"}),
		)
		if err != nil {
			kafkaErr = err
			return
		}
		onShutdown(func(ctx context.Context) { _ = container.Terminate(ctx) })
		kafkaBrokers, kafkaErr = container.Brokers(ctx)
	})
	if kafkaErr != nil {
		t.Fatalf("start Kafka (is Docker running? or set TEST_KAFKA_BROKERS): %v", kafkaErr)
	}
	return kafkaBrokers
}

func adminClient(t testing.TB) *segkafka.Client {
	return &segkafka.Client{Addr: segkafka.TCP(Brokers(t)...), Timeout: 10 * time.Second}
}

func CreateTopic(t testing.TB, name string) {
	t.Helper()
	resp, err := adminClient(t).CreateTopics(context.Background(), &segkafka.CreateTopicsRequest{
		Topics: []segkafka.TopicConfig{{Topic: name, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	if e := resp.Errors[name]; e != nil {
		t.Fatalf("create topic %s: %v", name, e)
	}
	waitForLeader(t, name)
}

func waitForLeader(t testing.TB, topic string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		md, err := adminClient(t).Metadata(context.Background(), &segkafka.MetadataRequest{Topics: []string{topic}})
		if err == nil && len(md.Topics) == 1 && md.Topics[0].Error == nil &&
			len(md.Topics[0].Partitions) == 1 && md.Topics[0].Partitions[0].Error == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %s never got a leader", topic)
}

func UniqueName(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}

func NewTopic(t testing.TB, prefix string) string {
	t.Helper()
	topic := UniqueName(prefix)
	CreateTopic(t, topic)
	CreateTopic(t, topic+".dlq")
	return topic
}

func Produce(t testing.TB, topic, key string, value []byte, headers ...segkafka.Header) {
	t.Helper()
	w := &segkafka.Writer{
		Addr:         segkafka.TCP(Brokers(t)...),
		Topic:        topic,
		Balancer:     &segkafka.Hash{},
		RequiredAcks: segkafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()
	err := w.WriteMessages(context.Background(), segkafka.Message{Key: []byte(key), Value: value, Headers: headers})
	if err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}

func EventType(v string) segkafka.Header {
	return segkafka.Header{Key: "event_type", Value: []byte(v)}
}

func Header(m segkafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func CommittedOffset(t testing.TB, group, topic string) int64 {
	t.Helper()
	resp, err := adminClient(t).OffsetFetch(context.Background(), &segkafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: {0}},
	})
	if err != nil || len(resp.Topics[topic]) != 1 {
		return -1
	}
	return resp.Topics[topic][0].CommittedOffset
}

func endOffset(t testing.TB, topic string) int64 {
	t.Helper()
	resp, err := adminClient(t).ListOffsets(context.Background(), &segkafka.ListOffsetsRequest{
		Topics: map[string][]segkafka.OffsetRequest{topic: {segkafka.LastOffsetOf(0)}},
	})
	if err != nil || len(resp.Topics[topic]) != 1 {
		t.Fatalf("list offsets for %s: %v", topic, err)
	}
	return resp.Topics[topic][0].LastOffset
}

func ReadAll(t testing.TB, topic string) []segkafka.Message {
	t.Helper()
	n := endOffset(t, topic)
	r := segkafka.NewReader(segkafka.ReaderConfig{Brokers: Brokers(t), Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer r.Close()
	if err := r.SetOffset(segkafka.FirstOffset); err != nil {
		t.Fatalf("set offset: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
