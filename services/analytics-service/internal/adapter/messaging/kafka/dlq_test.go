package kafka_test

import (
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
)

func TestDeadLetterRecordKeepsTheOriginalMessageAndSaysWhereItCameFrom(t *testing.T) {
	original := segkafka.Message{
		Topic: "user.events", Partition: 2, Offset: 41,
		Key: []byte("user-1"), Value: []byte(`{"broken":`),
		Headers: []segkafka.Header{{Key: "event_type", Value: []byte("UserCreated")}},
	}
	at := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	dead := kafka.DeadLetterRecord(original, "parse: unexpected end of JSON input", at)

	if string(dead.Key) != "user-1" || string(dead.Value) != `{"broken":` {
		t.Fatalf("key/value = %q/%q, want the original untouched", dead.Key, dead.Value)
	}
	headers := map[string]string{}
	for _, h := range dead.Headers {
		headers[h.Key] = string(h.Value)
	}
	want := map[string]string{
		"x-dlq-reason":           "parse: unexpected end of JSON input",
		"x-dlq-source-topic":     "user.events",
		"x-dlq-source-partition": "2",
		"x-dlq-source-offset":    "41",
		"x-dlq-at":               "2026-05-06T07:08:09Z",
	}
	for k, v := range want {
		if headers[k] != v {
			t.Fatalf("header %s = %q, want %q (all headers: %v)", k, headers[k], v, headers)
		}
	}
}
