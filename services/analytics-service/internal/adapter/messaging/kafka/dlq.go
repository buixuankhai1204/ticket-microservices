package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	segkafka "github.com/segmentio/kafka-go"
)

type DeadLetters interface {
	Park(ctx context.Context, m segkafka.Message, reason string) error
}

type KafkaDeadLetters struct {
	writer *segkafka.Writer
}

func NewKafkaDeadLetters(brokers []string, topic string) *KafkaDeadLetters {
	return &KafkaDeadLetters{
		writer: &segkafka.Writer{
			Addr:         segkafka.TCP(brokers...),
			Topic:        topic + ".dlq",
			Balancer:     &segkafka.Hash{},
			RequiredAcks: segkafka.RequireAll,
		},
	}
}

func (d *KafkaDeadLetters) Park(ctx context.Context, m segkafka.Message, reason string) error {
	if err := d.writer.WriteMessages(ctx, DeadLetterRecord(m, reason, time.Now().UTC())); err != nil {
		return fmt.Errorf("write to dlq: %w", err)
	}
	return nil
}

func (d *KafkaDeadLetters) Close() error {
	return d.writer.Close()
}

func DeadLetterRecord(m segkafka.Message, reason string, at time.Time) segkafka.Message {
	return segkafka.Message{
		Key:   m.Key,
		Value: m.Value,
		Headers: []segkafka.Header{
			{Key: "x-dlq-reason", Value: []byte(reason)},
			{Key: "x-dlq-source-topic", Value: []byte(m.Topic)},
			{Key: "x-dlq-source-partition", Value: []byte(strconv.Itoa(m.Partition))},
			{Key: "x-dlq-source-offset", Value: []byte(strconv.FormatInt(m.Offset, 10))},
			{Key: "x-dlq-at", Value: []byte(at.Format(time.RFC3339))},
		},
	}
}
