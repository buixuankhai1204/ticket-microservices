package kafka

import (
	"context"
	"errors"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
)

// Recorder is the use case a consumer drives: it owns the transaction and the
// processed_events idempotency check, returning alreadyProcessed=true for a
// duplicate.
type Recorder[E any] interface {
	Execute(ctx context.Context, ev E) (alreadyProcessed bool, err error)
}

// EventSpec is everything that differs between the consumer groups sharing one
// topic. The engine below is otherwise identical for every event type.
type EventSpec[E any] struct {
	Group      string                  // Kafka consumer group id
	EventType  string                  // "event_type" header value this group owns
	Component  string                  // logger component tag
	SuccessMsg string                  // logged on a fresh (non-duplicate) apply
	Parse      func([]byte) (E, error) // wire bytes -> domain event
	LogFields  func(E) []any           // structured log context, e.g. event_id/user_id
	Record     Recorder[E]
}

type Config struct {
	Brokers     []string
	Topic       string
	MaxAttempts int
}

type Consumer[E any] struct {
	reader    *segkafka.Reader
	dlq       *KafkaDeadLetters
	processor *Processor[E]
	group     string
	log       logger.Logger
}

func NewConsumer[E any](cfg Config, spec EventSpec[E], log logger.Logger) *Consumer[E] {
	dlq := NewKafkaDeadLetters(cfg.Brokers, cfg.Topic)
	log = log.With("topic", cfg.Topic)
	return &Consumer[E]{
		reader: segkafka.NewReader(segkafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        spec.Group,
			Topic:          cfg.Topic,
			MinBytes:       1,
			MaxBytes:       10 * 1024 * 1024,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: 0,
		}),
		dlq:       dlq,
		processor: NewProcessor(spec, dlq, DefaultRetryPolicy(cfg.MaxAttempts), log),
		group:     spec.Group,
		log:       log.With("component", spec.Component),
	}
}

func (c *Consumer[E]) Run(ctx context.Context) error {
	c.log.Info("consumer started", "group", c.group, "max_attempts", c.processor.policy.MaxAttempts)

	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("consumer stopping")
				return nil
			}
			c.log.Error("fetch failed, retrying", "err", err.Error())
			if sleep(ctx, time.Second) != nil {
				return nil
			}
			continue
		}

		for {
			_, err := c.processor.Process(ctx, m)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("message not processed, retrying in place", "err", err.Error(), "offset", m.Offset)
			if sleep(ctx, time.Second) != nil {
				return nil
			}
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("commit failed; message may be redelivered", "err", err.Error(), "offset", m.Offset)
		}
	}
}

func (c *Consumer[E]) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
