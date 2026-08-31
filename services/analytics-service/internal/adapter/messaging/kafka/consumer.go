package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	segkafka "github.com/segmentio/kafka-go"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/logger"
)

const consumerGroup = "analytics-service-UserCreated"

type userCreatedRecorder interface {
	Execute(ctx context.Context, ev domain.UserCreated) (alreadyProcessed bool, err error)
}

type Config struct {
	Brokers     []string
	Topic       string
	MaxAttempts int
}

type Consumer struct {
	reader      *segkafka.Reader
	dlq         *segkafka.Writer
	record      userCreatedRecorder
	log         logger.Logger
	maxAttempts int
}

func NewConsumer(cfg Config, record userCreatedRecorder, log logger.Logger) *Consumer {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 5
	}
	return &Consumer{
		reader: segkafka.NewReader(segkafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        consumerGroup,
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
		record:      record,
		log:         log.With("component", "user_created_consumer", "topic", cfg.Topic),
		maxAttempts: maxAttempts,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("consumer started", "group", consumerGroup, "max_attempts", c.maxAttempts)

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

func (c *Consumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

func (c *Consumer) handle(ctx context.Context, m segkafka.Message) error {
	if t := headerValue(m, "event_type"); t != "" && t != "UserCreated" {
		c.log.Error("unexpected event_type -> dlq", "event_type", t, "offset", m.Offset)
		return c.toDLQ(ctx, m, "unexpected event_type: "+t)
	}

	ev, parseErr := parseUserCreated(m.Value)
	if parseErr != nil {
		c.log.Error("undeserializable message -> dlq", "err", parseErr.Error(), "offset", m.Offset)
		return c.toDLQ(ctx, m, "parse: "+parseErr.Error())
	}

	log := c.log.With("event_id", ev.EventID.String(), "user_id", ev.UserID.String(), "offset", m.Offset)
	backoff := 250 * time.Millisecond

	for attempt := 1; ; attempt++ {
		already, err := c.record.Execute(ctx, ev)
		switch {
		case err == nil:
			if already {
				log.Info("duplicate event skipped")
			} else {
				log.Info("user registration recorded")
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

func (c *Consumer) toDLQ(ctx context.Context, m segkafka.Message, reason string) error {
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

type userCreatedWire struct {
	EventID   string `json:"event_id"`
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

func parseUserCreated(b []byte) (domain.UserCreated, error) {
	var w userCreatedWire
	if err := json.Unmarshal(b, &w); err != nil {
		return domain.UserCreated{}, fmt.Errorf("unmarshal UserCreated: %w", err)
	}
	eventID, err := uuid.Parse(w.EventID)
	if err != nil {
		return domain.UserCreated{}, fmt.Errorf("bad event_id %q: %w", w.EventID, err)
	}
	userID, err := uuid.Parse(w.UserID)
	if err != nil {
		return domain.UserCreated{}, fmt.Errorf("bad user_id %q: %w", w.UserID, err)
	}
	createdAt, err := time.Parse(time.RFC3339, w.CreatedAt)
	if err != nil {
		return domain.UserCreated{}, fmt.Errorf("bad created_at %q: %w", w.CreatedAt, err)
	}
	return domain.UserCreated{
		EventID:   eventID,
		UserID:    userID,
		Email:     w.Email,
		CreatedAt: createdAt,
	}, nil
}
