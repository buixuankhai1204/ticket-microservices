package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
)

type Recorder[E any] interface {
	Execute(ctx context.Context, ev E) (alreadyProcessed bool, err error)
}

type EventSpec[E any] struct {
	Group      string
	EventType  string
	Component  string
	SuccessMsg string
	Parse      func([]byte) (E, error)
	LogFields  func(E) []any
	Record     Recorder[E]
}

type Config struct {
	Brokers     []string
	Topic       string
	MaxAttempts int
}

type Consumer[E any] struct {
	reader      *segkafka.Reader
	dlq         *segkafka.Writer
	spec        EventSpec[E]
	log         logger.Logger
	maxAttempts int
}

func NewConsumer[E any](cfg Config, spec EventSpec[E], log logger.Logger) *Consumer[E] {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 5
	}
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
		dlq: &segkafka.Writer{
			Addr:         segkafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic + ".dlq",
			Balancer:     &segkafka.Hash{},
			RequiredAcks: segkafka.RequireAll,
		},
		spec:        spec,
		log:         log.With("component", spec.Component, "topic", cfg.Topic),
		maxAttempts: maxAttempts,
	}
}

func (c *Consumer[E]) Run(ctx context.Context) error {
	c.log.Info("consumer started", "group", c.spec.Group, "max_attempts", c.maxAttempts)

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

		if err := c.handle(ctx, m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("message not processed, will be redelivered", "err", err.Error(), "offset", m.Offset)
			if sleep(ctx, time.Second) != nil {
				return nil
			}
			continue
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

func (c *Consumer[E]) handle(ctx context.Context, m segkafka.Message) error {
	if t := headerValue(m, "event_type"); t != "" && t != c.spec.EventType {
		c.log.Info("event_type not handled by this consumer, skipping", "event_type", t, "offset", m.Offset)
		return nil
	}

	ev, parseErr := c.spec.Parse(m.Value)
	if parseErr != nil {
		c.log.Error("undeserializable message -> dlq", "err", parseErr.Error(), "offset", m.Offset)
		return c.toDLQ(ctx, m, "parse: "+parseErr.Error())
	}

	log := c.log.With(append(c.spec.LogFields(ev), "offset", m.Offset)...)
	backoff := 250 * time.Millisecond

	for attempt := 1; ; attempt++ {
		already, err := c.spec.Record.Execute(ctx, ev)
		switch {
		case err == nil:
			if already {
				log.Info("duplicate event skipped")
			} else {
				log.Info(c.spec.SuccessMsg)
			}
			return nil

		case ctx.Err() != nil:
			return ctx.Err()

		case isRetryable(err):
			if attempt >= c.maxAttempts {
				log.Error("max retries exhausted -> dlq", "attempts", attempt, "err", err.Error())
				return c.toDLQ(ctx, m, fmt.Sprintf("max-retries after %d attempts: %v", attempt, err))
			}
			log.Info("retryable error, backing off", "attempt", attempt, "backoff_ms", backoff.Milliseconds(), "err", err.Error())
			if sleep(ctx, backoff) != nil {
				return ctx.Err()
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}

		default:
			log.Error("permanent error -> dlq", "err", err.Error())
			return c.toDLQ(ctx, m, "permanent: "+err.Error())
		}
	}
}

func (c *Consumer[E]) toDLQ(ctx context.Context, m segkafka.Message, reason string) error {
	dead := segkafka.Message{
		Key:   m.Key,
		Value: m.Value,
		Headers: []segkafka.Header{
			{Key: "x-dlq-reason", Value: []byte(reason)},
			{Key: "x-dlq-source-topic", Value: []byte(m.Topic)},
			{Key: "x-dlq-source-partition", Value: []byte(strconv.Itoa(m.Partition))},
			{Key: "x-dlq-source-offset", Value: []byte(strconv.FormatInt(m.Offset, 10))},
			{Key: "x-dlq-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}
	if err := c.dlq.WriteMessages(ctx, dead); err != nil {
		return fmt.Errorf("write to dlq: %w", err)
	}
	c.log.Error("message dead-lettered", "reason", reason, "offset", m.Offset)
	return nil
}

func isRetryable(err error) bool {
	var repoErr *domain.RepositoryError
	return errors.As(err, &repoErr)
}

func headerValue(m segkafka.Message, key string) string {
	for _, h := range m.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
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
