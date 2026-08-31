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

const userLoggedInConsumerGroup = "analytics-service-UserLoggedIn"

type userLoggedInRecorder interface {
	Execute(ctx context.Context, ev domain.UserLoggedIn) (alreadyProcessed bool, err error)
}

// UserLoggedInConfig mirrors Config; kept separate so the two consumers on
// user.events can be tuned independently.
type UserLoggedInConfig struct {
	Brokers     []string
	Topic       string
	MaxAttempts int
}

type UserLoggedInConsumer struct {
	reader      *segkafka.Reader
	dlq         *segkafka.Writer
	record      userLoggedInRecorder
	log         logger.Logger
	maxAttempts int
}

func NewUserLoggedInConsumer(cfg UserLoggedInConfig, record userLoggedInRecorder, log logger.Logger) *UserLoggedInConsumer {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 5
	}
	return &UserLoggedInConsumer{
		reader: segkafka.NewReader(segkafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        userLoggedInConsumerGroup,
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
		log:         log.With("component", "user_logged_in_consumer", "topic", cfg.Topic),
		maxAttempts: maxAttempts,
	}
}

func (c *UserLoggedInConsumer) Run(ctx context.Context) error {
	c.log.Info("consumer started", "group", userLoggedInConsumerGroup, "max_attempts", c.maxAttempts)

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

func (c *UserLoggedInConsumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

func (c *UserLoggedInConsumer) handle(ctx context.Context, m segkafka.Message) error {
	// user.events carries every `user` aggregate event (UserCreated, UserLoggedIn,
	// ...). Anything that isn't ours belongs to a sibling consumer group — ack it
	// and move on, never dead-letter it.
	if t := headerValue(m, "event_type"); t != "" && t != "UserLoggedIn" {
		c.log.Info("event_type not handled by this consumer, skipping", "event_type", t, "offset", m.Offset)
		return nil
	}

	ev, parseErr := parseUserLoggedIn(m.Value)
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
				log.Info("user login recorded")
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

func (c *UserLoggedInConsumer) toDLQ(ctx context.Context, m segkafka.Message, reason string) error {
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

type userLoggedInWire struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	Email      string `json:"email"`
	LoggedInAt string `json:"logged_in_at"`
}

func parseUserLoggedIn(b []byte) (domain.UserLoggedIn, error) {
	var w userLoggedInWire
	if err := json.Unmarshal(b, &w); err != nil {
		return domain.UserLoggedIn{}, fmt.Errorf("unmarshal UserLoggedIn: %w", err)
	}
	eventID, err := uuid.Parse(w.EventID)
	if err != nil {
		return domain.UserLoggedIn{}, fmt.Errorf("bad event_id %q: %w", w.EventID, err)
	}
	userID, err := uuid.Parse(w.UserID)
	if err != nil {
		return domain.UserLoggedIn{}, fmt.Errorf("bad user_id %q: %w", w.UserID, err)
	}
	loggedInAt, err := time.Parse(time.RFC3339, w.LoggedInAt)
	if err != nil {
		return domain.UserLoggedIn{}, fmt.Errorf("bad logged_in_at %q: %w", w.LoggedInAt, err)
	}
	return domain.UserLoggedIn{
		EventID:    eventID,
		UserID:     userID,
		Email:      w.Email,
		LoggedInAt: loggedInAt,
	}, nil
}
