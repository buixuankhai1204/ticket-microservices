// Package kafka holds analytics-service's inbound saga adapter: it consumes
// events other services publish and drives a local use case for each. It never
// publishes saga events (analytics is a read model) — the only topic it writes
// to is its own dead-letter topic.
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

// consumerGroup follows the repo convention <service-name>-<EventName>. One
// group per (service, event) so each event stream is tracked independently.
const consumerGroup = "analytics-service-UserCreated"

// userCreatedRecorder is the slice of the record use case this consumer needs,
// as an interface so it can be faked in tests.
type userCreatedRecorder interface {
	Execute(ctx context.Context, ev domain.UserCreated) (alreadyProcessed bool, err error)
}

// Config is the consumer's runtime configuration, resolved in platform/config.
type Config struct {
	Brokers     []string
	Topic       string
	MaxAttempts int
}

// Consumer subscribes to the UserCreated topic and projects each event into the
// read model via the injected use case.
//
//   - Offsets are committed manually, only after a message is fully handled
//     (FetchMessage + CommitMessages) — at-least-once, as the outbox relay on the
//     publish side is also at-least-once.
//   - Idempotency is enforced downstream by the use case (processed_events table),
//     so a redelivery is a harmless no-op.
//   - A message that can't be deserialized, or that fails permanently, or that
//     keeps failing past MaxAttempts, is routed to "<topic>.dlq" and its offset
//     committed, so one poison record can't wedge the partition.
type Consumer struct {
	reader      *segkafka.Reader
	dlq         *segkafka.Writer
	record      userCreatedRecorder
	log         logger.Logger
	maxAttempts int
}

// NewConsumer builds the consumer. It does not connect until Run is called.
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
			CommitInterval: 0, // 0 => CommitMessages commits synchronously
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

// Run polls until ctx is cancelled, then returns nil. It only returns after a
// clean stop; transient broker / commit failures are logged and retried rather
// than propagated, so a blip doesn't take the process down.
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
			// handle only errors when it could not even dead-letter the message
			// (DLQ write failed). Don't commit — let it be redelivered — and
			// pause briefly so we don't hot-loop a sick broker.
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
			// Not fatal: the message will be redelivered and the use case is
			// idempotent.
			c.log.Error("commit failed; message may be redelivered", "err", err.Error(), "offset", m.Offset)
		}
	}
}

// Close releases the reader and DLQ writer. Safe to call once, after Run returns.
func (c *Consumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

// handle takes one message to a terminal state: applied, skipped as a duplicate,
// or dead-lettered. In all three it returns nil and the caller commits the
// offset. It returns a non-nil error only when the message could not be
// dead-lettered (so the offset must NOT be committed) or ctx was cancelled.
func (c *Consumer) handle(ctx context.Context, m segkafka.Message) error {
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
			// Permanent domain rejection (e.g. ErrInvalidUserRegistration) —
			// retrying can't help.
			log.Error("permanent error -> dlq", "err", err.Error())
			return c.toDLQ(ctx, m, "permanent: "+err.Error())
		}
	}
}

// toDLQ republishes the original message to the dead-letter topic with
// diagnostic headers. A failure here means we can't dead-letter, so the caller
// keeps the offset uncommitted for redelivery.
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

// isRetryable reports whether err is an infrastructure failure worth retrying.
// Only *domain.RepositoryError (DB / network blips) qualifies; a domain rule
// violation is permanent.
func isRetryable(err error) bool {
	var repoErr *domain.RepositoryError
	return errors.As(err, &repoErr)
}

// sleep waits for d, or returns early with ctx.Err() if ctx is cancelled first.
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

// userCreatedWire is the on-the-wire JSON shape published by user-service
// (rdkafka, serde). Field names are snake_case; timestamps are RFC 3339.
type userCreatedWire struct {
	EventID   string `json:"event_id"`
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	CreatedAt string `json:"created_at"`
}

// parseUserCreated validates the transport shape only (well-formed JSON, real
// UUIDs, a parseable timestamp). Business validation of the fields is the domain
// constructor's job, invoked later by the use case.
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
