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

func BookingConfirmedSpec(record Recorder[domain.BookingConfirmed]) EventSpec[domain.BookingConfirmed] {
	return EventSpec[domain.BookingConfirmed]{
		Group:      "analytics-service-BookingConfirmed",
		EventType:  "BookingConfirmed",
		Component:  "booking_confirmed_consumer",
		SuccessMsg: "booking outcome recorded",
		Parse:      parseBookingConfirmed,
		LogFields: func(ev domain.BookingConfirmed) []any {
			return []any{"event_id", ev.EventID.String(), "booking_id", ev.BookingID.String()}
		},
		Record: record,
	}
}

func BookingCancelledSpec(record Recorder[domain.BookingCancelled]) EventSpec[domain.BookingCancelled] {
	return EventSpec[domain.BookingCancelled]{
		Group:      "analytics-service-BookingCancelled",
		EventType:  "BookingCancelled",
		Component:  "booking_cancelled_consumer",
		SuccessMsg: "booking outcome recorded",
		Parse:      parseBookingCancelled,
		LogFields: func(ev domain.BookingCancelled) []any {
			return []any{"event_id", ev.EventID.String(), "booking_id", ev.BookingID.String()}
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

type bookingOutcomeWire struct {
	EventID         string `json:"event_id"`
	BookingID       string `json:"booking_id"`
	TicketedEventID string `json:"ticketed_event_id"`
	OccurredAt      string `json:"occurred_at"`
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

func decodeBookingOutcome(eventName string, b []byte) (eventID, bookingID, ticketedEventID uuid.UUID, occurredAt time.Time, err error) {
	var w bookingOutcomeWire
	if err = json.Unmarshal(b, &w); err != nil {
		return uuid.UUID{}, uuid.UUID{}, uuid.UUID{}, time.Time{}, fmt.Errorf("unmarshal %s: %w", eventName, err)
	}
	if eventID, err = parseUUID("event_id", w.EventID); err != nil {
		return uuid.UUID{}, uuid.UUID{}, uuid.UUID{}, time.Time{}, err
	}
	if bookingID, err = parseUUID("booking_id", w.BookingID); err != nil {
		return uuid.UUID{}, uuid.UUID{}, uuid.UUID{}, time.Time{}, err
	}
	if ticketedEventID, err = parseUUID("ticketed_event_id", w.TicketedEventID); err != nil {
		return uuid.UUID{}, uuid.UUID{}, uuid.UUID{}, time.Time{}, err
	}
	if occurredAt, err = parseRFC3339("occurred_at", w.OccurredAt); err != nil {
		return uuid.UUID{}, uuid.UUID{}, uuid.UUID{}, time.Time{}, err
	}
	return eventID, bookingID, ticketedEventID, occurredAt, nil
}

func parseBookingConfirmed(b []byte) (domain.BookingConfirmed, error) {
	eventID, bookingID, ticketedEventID, occurredAt, err := decodeBookingOutcome("BookingConfirmed", b)
	if err != nil {
		return domain.BookingConfirmed{}, err
	}
	return domain.BookingConfirmed{
		EventID:         eventID,
		BookingID:       bookingID,
		TicketedEventID: ticketedEventID,
		OccurredAt:      occurredAt,
	}, nil
}

func parseBookingCancelled(b []byte) (domain.BookingCancelled, error) {
	eventID, bookingID, ticketedEventID, occurredAt, err := decodeBookingOutcome("BookingCancelled", b)
	if err != nil {
		return domain.BookingCancelled{}, err
	}
	return domain.BookingCancelled{
		EventID:         eventID,
		BookingID:       bookingID,
		TicketedEventID: ticketedEventID,
		OccurredAt:      occurredAt,
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
