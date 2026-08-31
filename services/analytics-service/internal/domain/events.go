package domain

import (
	"time"

	"github.com/google/uuid"
)

type UserCreated struct {
	EventID   uuid.UUID
	UserID    uuid.UUID
	Email     string
	CreatedAt time.Time
}

type UserLoggedIn struct {
	EventID    uuid.UUID
	UserID     uuid.UUID
	Email      string
	LoggedInAt time.Time
}
