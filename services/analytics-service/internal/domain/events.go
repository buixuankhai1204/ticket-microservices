package domain

import (
	"time"

	"github.com/google/uuid"
)

// UserCreated is the event user-service publishes on the `user.created` topic
// once a user has been durably persisted. It is a plain value — the messaging
// adapter deserializes the wire JSON into this; domain knows nothing about Kafka.
//
// EventID is a per-event UUID used as the idempotency key (see processed_events).
// UserID is the aggregate id and the Kafka partition key.
type UserCreated struct {
	EventID   uuid.UUID
	UserID    uuid.UUID
	Email     string
	CreatedAt time.Time
}
