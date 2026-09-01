package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

func UserCreatedSpec(record Recorder[domain.UserCreated]) EventSpec[domain.UserCreated] {
	return EventSpec[domain.UserCreated]{
		Group:      "analytics-service-UserCreated",
		EventType:  "UserCreated",
		Component:  "user_created_consumer",
		SuccessMsg: "user registration recorded",
		Parse:      parseUserCreated,
		LogFields: func(ev domain.UserCreated) []any {
			return []any{"event_id", ev.EventID.String(), "user_id", ev.UserID.String()}
		},
		Record: record,
	}
}

func UserLoggedInSpec(record Recorder[domain.UserLoggedIn]) EventSpec[domain.UserLoggedIn] {
	return EventSpec[domain.UserLoggedIn]{
		Group:      "analytics-service-UserLoggedIn",
		EventType:  "UserLoggedIn",
		Component:  "user_logged_in_consumer",
		SuccessMsg: "user login recorded",
		Parse:      parseUserLoggedIn,
		LogFields: func(ev domain.UserLoggedIn) []any {
			return []any{"event_id", ev.EventID.String(), "user_id", ev.UserID.String()}
		},
		Record: record,
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
	eventID, err := parseUUID("event_id", w.EventID)
	if err != nil {
		return domain.UserCreated{}, err
	}
	userID, err := parseUUID("user_id", w.UserID)
	if err != nil {
		return domain.UserCreated{}, err
	}
	createdAt, err := parseRFC3339("created_at", w.CreatedAt)
	if err != nil {
		return domain.UserCreated{}, err
	}
	return domain.UserCreated{
		EventID:   eventID,
		UserID:    userID,
		Email:     w.Email,
		CreatedAt: createdAt,
	}, nil
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
	eventID, err := parseUUID("event_id", w.EventID)
	if err != nil {
		return domain.UserLoggedIn{}, err
	}
	userID, err := parseUUID("user_id", w.UserID)
	if err != nil {
		return domain.UserLoggedIn{}, err
	}
	loggedInAt, err := parseRFC3339("logged_in_at", w.LoggedInAt)
	if err != nil {
		return domain.UserLoggedIn{}, err
	}
	return domain.UserLoggedIn{
		EventID:    eventID,
		UserID:     userID,
		Email:      w.Email,
		LoggedInAt: loggedInAt,
	}, nil
}

func parseUUID(field, s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("bad %s %q: %w", field, s, err)
	}
	return id, nil
}

func parseRFC3339(field, s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("bad %s %q: %w", field, s, err)
	}
	return t, nil
}
