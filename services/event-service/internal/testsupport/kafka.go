package testsupport

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	segkafka "github.com/segmentio/kafka-go"
)

func Brokers() []string {
	if v := os.Getenv("KAFKA_TEST_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9094"}
}

func adminClient() *segkafka.Client {
	return &segkafka.Client{Addr: segkafka.TCP(Brokers()...), Timeout: 10 * time.Second}
}

func CreateTopic(t *testing.T, name string) {
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
	Eventually(t, 15*time.Second, "topic "+name+" to have a leader", func() bool {
		md, err := adminClient().Metadata(context.Background(), &segkafka.MetadataRequest{Topics: []string{name}})
		if err != nil || len(md.Topics) != 1 || md.Topics[0].Error != nil || len(md.Topics[0].Partitions) != 1 {
			return false
		}
		return md.Topics[0].Partitions[0].Error == nil
	})
}

func NewTopic(t *testing.T) string {
	t.Helper()
	topic := "comp-" + uuid.NewString()[:8]
	CreateTopic(t, topic)
	CreateTopic(t, topic+".dlq")
	return topic
}

func Produce(t *testing.T, topic, key, value, eventType string) {
	t.Helper()
	w := &segkafka.Writer{
		Addr:         segkafka.TCP(Brokers()...),
		Topic:        topic,
		Balancer:     &segkafka.Hash{},
		RequiredAcks: segkafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()
	err := w.WriteMessages(context.Background(), segkafka.Message{
		Key:     []byte(key),
		Value:   []byte(value),
		Headers: []segkafka.Header{{Key: "event_type", Value: []byte(eventType)}},
	})
	if err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}
