package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	segkafka "github.com/segmentio/kafka-go"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
)

// retryableSQLSTATEs are transient at the database level: the same statement
// against the same input could succeed on a later attempt. Everything else
// (constraint/data/schema errors) is deterministic -- retrying only burns the
// backoff ladder before an inevitable DLQ, so it goes straight there instead.
var retryableSQLSTATEs = map[string]bool{
	"40001": true, // serialization_failure
	"40P01": true, // deadlock_detected
	"55P03": true, // lock_not_available
	"55006": true, // object_in_use
	"53300": true, // too_many_connections
	"08000": true, // connection_exception
	"08001": true, // sqlclient_unable_to_establish_sqlconnection
	"08003": true, // connection_does_not_exist
	"08004": true, // sqlserver_rejected_establishment_of_sqlconnection
	"08006": true, // connection_failure
	"08007": true, // transaction_resolution_unknown
	"08P01": true, // protocol_violation
	"57P01": true, // admin_shutdown
	"57P02": true, // crash_shutdown
	"57P03": true, // cannot_connect_now
}

type Outcome int

const (
	Handled Outcome = iota
	Ignored
	Parked
)

func (o Outcome) String() string {
	switch o {
	case Handled:
		return "handled"
	case Ignored:
		return "ignored"
	case Parked:
		return "parked"
	default:
		return "unknown"
	}
}

type Handler interface {
	Process(ctx context.Context, m segkafka.Message) (Outcome, error)
}

type Processor[E any] struct {
	spec   EventSpec[E]
	dlq    DeadLetters
	policy RetryPolicy
	log    logger.Logger
}

func NewProcessor[E any](spec EventSpec[E], dlq DeadLetters, policy RetryPolicy, log logger.Logger) *Processor[E] {
	return &Processor[E]{spec: spec, dlq: dlq, policy: policy, log: log.With("component", spec.Component)}
}

func (p *Processor[E]) Process(ctx context.Context, m segkafka.Message) (Outcome, error) {
	if t := headerValue(m, "event_type"); t != "" && t != p.spec.EventType {
		p.log.Info("event_type not handled by this consumer, skipping", "event_type", t, "offset", m.Offset)
		return Ignored, nil
	}

	ev, parseErr := p.spec.Parse(m.Value)
	if parseErr != nil {
		p.log.Error("undeserializable message -> dlq", "err", parseErr.Error(), "offset", m.Offset)
		return p.park(ctx, m, "parse: "+parseErr.Error())
	}

	log := p.log.With(append(p.spec.LogFields(ev), "offset", m.Offset)...)

	for attempt := 1; ; attempt++ {
		already, err := p.spec.Record.Execute(ctx, ev)
		switch {
		case err == nil:
			if already {
				log.Info("duplicate event skipped")
			} else {
				log.Info(p.spec.SuccessMsg)
			}
			return Handled, nil

		case ctx.Err() != nil:
			return Handled, ctx.Err()

		case isRetryable(err):
			if attempt >= p.policy.MaxAttempts {
				log.Error("max retries exhausted -> dlq", "attempts", attempt, "err", err.Error())
				return p.park(ctx, m, fmt.Sprintf("max-retries after %d attempts: %v", attempt, err))
			}
			wait := p.policy.Backoff(attempt)
			log.Info("retryable error, backing off", "attempt", attempt, "backoff_ms", wait.Milliseconds(), "err", err.Error())
			if sleep(ctx, wait) != nil {
				return Handled, ctx.Err()
			}

		default:
			log.Error("permanent error -> dlq", "err", err.Error())
			return p.park(ctx, m, "permanent: "+err.Error())
		}
	}
}

func (p *Processor[E]) park(ctx context.Context, m segkafka.Message, reason string) (Outcome, error) {
	if err := p.dlq.Park(ctx, m, reason); err != nil {
		return Handled, err
	}
	p.log.Error("message dead-lettered", "reason", reason, "offset", m.Offset)
	return Parked, nil
}

func isRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return retryableSQLSTATEs[pgErr.Code]
	}
	// Not a database error at all (pool-acquire timeout, context deadline,
	// broken connection): the repo still wrapped it in RepositoryError, and
	// there's no reason to believe a retry can't succeed, so treat it as
	// transient the same way the pre-SQLSTATE classifier did.
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
