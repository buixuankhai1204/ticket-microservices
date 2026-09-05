package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

func BookingRequestedSpec(record Recorder[domain.BookingRequested]) EventSpec[domain.BookingRequested] {
	return EventSpec[domain.BookingRequested]{
		Group:      "event-service-BookingRequested",
		EventType:  "BookingRequested",
		Component:  "booking_requested_consumer",
		SuccessMsg: "seat reservation processed",
		Parse:      parseBookingRequested,
		LogFields: func(ev domain.BookingRequested) []any {
			return []any{"event_id", ev.ID.String(), "booking_id", ev.BookingID.String()}
		},
		Record: record,
	}
}

type bookingRequestedWire struct {
	EventID         string   `json:"event_id"`
	BookingID       string   `json:"booking_id"`
	UserID          string   `json:"user_id"`
	TicketedEventID string   `json:"ticketed_event_id"`
	SeatIDs         []string `json:"seat_ids"`
	RequestedAt     string   `json:"requested_at"`
}

func parseBookingRequested(b []byte) (domain.BookingRequested, error) {
	var w bookingRequestedWire
	if err := json.Unmarshal(b, &w); err != nil {
		return domain.BookingRequested{}, fmt.Errorf("unmarshal BookingRequested: %w", err)
	}

	eventID, err := parseUUID("event_id", w.EventID)
	if err != nil {
		return domain.BookingRequested{}, err
	}
	bookingID, err := parseUUID("booking_id", w.BookingID)
	if err != nil {
		return domain.BookingRequested{}, err
	}
	userID, err := parseUUID("user_id", w.UserID)
	if err != nil {
		return domain.BookingRequested{}, err
	}
	ticketedEventID, err := parseUUID("ticketed_event_id", w.TicketedEventID)
	if err != nil {
		return domain.BookingRequested{}, err
	}

	seatIDs := make([]uuid.UUID, 0, len(w.SeatIDs))
	for _, s := range w.SeatIDs {
		id, err := parseUUID("seat_ids", s)
		if err != nil {
			return domain.BookingRequested{}, err
		}
		seatIDs = append(seatIDs, id)
	}
	if len(seatIDs) == 0 {
		return domain.BookingRequested{}, fmt.Errorf("seat_ids is empty")
	}

	requestedAt, err := parseRFC3339("requested_at", w.RequestedAt)
	if err != nil {
		return domain.BookingRequested{}, err
	}

	return domain.BookingRequested{
		ID:              eventID,
		BookingID:       bookingID,
		UserID:          userID,
		TicketedEventID: ticketedEventID,
		SeatIDs:         seatIDs,
		RequestedAt:     requestedAt,
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
