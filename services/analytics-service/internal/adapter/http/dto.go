package http

import (
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

type EventStatsResponse struct {
	EventID   uuid.UUID `json:"event_id"`
	Confirmed int64     `json:"confirmed"`
	Cancelled int64     `json:"cancelled"`
}

type UserRegistrationResponse struct {
	UserID       uuid.UUID `json:"user_id"`
	Email        string    `json:"email"`
	RegisteredAt time.Time `json:"registered_at"`
	RecordedAt   time.Time `json:"recorded_at"`
}

func ToUserRegistrationResponse(r domain.UserRegistration) UserRegistrationResponse {
	return UserRegistrationResponse{
		UserID:       r.UserID,
		Email:        r.Email,
		RegisteredAt: r.RegisteredAt,
		RecordedAt:   r.RecordedAt,
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func ToEventStatsResponse(s domain.EventBookingStats) EventStatsResponse {
	return EventStatsResponse{
		EventID:   s.EventID,
		Confirmed: s.Confirmed,
		Cancelled: s.Cancelled,
	}
}
