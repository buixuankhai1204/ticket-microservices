package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

func TestSeatReservationOutcomesAreRoutedByBookingAndPublishedWithTheAgreedFields(t *testing.T) {
	bookingID, eventID, seatID := uuid.New(), uuid.New(), uuid.New()
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	reserved := domain.NewSeatReservedEvent(bookingID, eventID, []uuid.UUID{seatID}, at)
	failed := domain.NewSeatReservationFailedEvent(bookingID, eventID, []uuid.UUID{seatID}, domain.ReasonSeatUnavailable, at)

	tests := []struct {
		name      string
		event     domain.OutboxEvent
		eventType string
		wantKeys  map[string]any
	}{
		{"reserved", reserved, "SeatReserved", map[string]any{
			"event_id": reserved.ID.String(), "booking_id": bookingID.String(), "ticketed_event_id": eventID.String(),
			"seat_ids": []any{seatID.String()}, "reserved_at": "2030-01-02T03:04:05Z",
		}},
		{"failed", failed, "SeatReservationFailed", map[string]any{
			"event_id": failed.ID.String(), "booking_id": bookingID.String(), "ticketed_event_id": eventID.String(),
			"seat_ids": []any{seatID.String()}, "reason": "seat_unavailable", "failed_at": "2030-01-02T03:04:05Z",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.event.EventType() != tc.eventType || tc.event.AggregateType() != "seat_reservation" {
				t.Fatalf("type = %s on %s, want %s on seat_reservation", tc.event.EventType(), tc.event.AggregateType(), tc.eventType)
			}
			if tc.event.AggregateID() != bookingID {
				t.Fatalf("aggregate id = %s, want the booking id so one booking's events stay in order", tc.event.AggregateID())
			}
			raw, err := json.Marshal(tc.event)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			want, _ := json.Marshal(tc.wantKeys)
			gotRaw, _ := json.Marshal(got)
			if string(want) != string(gotRaw) {
				t.Fatalf("payload = %s, want %s", gotRaw, want)
			}
		})
	}
}
