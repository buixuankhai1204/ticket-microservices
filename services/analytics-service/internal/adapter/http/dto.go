package http

import (
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

// EventStatsResponse is the wire shape for GET /api/v1/analytics/events/{eventID}.
type EventStatsResponse struct {
	EventID   uuid.UUID `json:"event_id"`
	Confirmed int64     `json:"confirmed"`
	Cancelled int64     `json:"cancelled"`
}

// UserRegistrationResponse is the wire shape for
// GET /api/v1/analytics/users/{userID}.
type UserRegistrationResponse struct {
	UserID       uuid.UUID `json:"user_id"`
	Email        string    `json:"email"`
	RegisteredAt time.Time `json:"registered_at"`
	RecordedAt   time.Time `json:"recorded_at"`
}

// ToUserRegistrationResponse is the one domain -> wire mapper for a user
// registration, kept next to the DTO (CLAUDE.md convention).
func ToUserRegistrationResponse(r domain.UserRegistration) UserRegistrationResponse {
	return UserRegistrationResponse{
		UserID:       r.UserID,
		Email:        r.Email,
		RegisteredAt: r.RegisteredAt,
		RecordedAt:   r.RecordedAt,
	}
}

// ErrorResponse is the single error envelope every handler returns.
type ErrorResponse struct {
	Error string `json:"error"`
}

// ToEventStatsResponse is the one domain -> wire mapper for event stats, kept
// next to the DTO (CLAUDE.md convention) so the mapping lives in one place
// instead of being re-derived inline per handler.
func ToEventStatsResponse(s domain.EventBookingStats) EventStatsResponse {
	return EventStatsResponse{
		EventID:   s.EventID,
		Confirmed: s.Confirmed,
		Cancelled: s.Cancelled,
	}
}
