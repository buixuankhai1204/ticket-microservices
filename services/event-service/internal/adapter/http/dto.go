package http

import (
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

// EventResponse is the wire shape for a single event.
type EventResponse struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Venue       string    `json:"venue"`
	StartsAt    time.Time `json:"starts_at"`
	EndsAt      time.Time `json:"ends_at"`
	CreatedAt   time.Time `json:"created_at"`
}

// SeatResponse is the wire shape for a single seat.
type SeatResponse struct {
	ID         uuid.UUID `json:"id"`
	EventID    uuid.UUID `json:"event_id"`
	Section    string    `json:"section"`
	Row        string    `json:"row"`
	Number     string    `json:"number"`
	Status     string    `json:"status"`
	PriceMinor int64     `json:"price_minor"`
}

// PaginationMeta is the envelope every list endpoint returns alongside its
// data, so a client can tell a response is paginated without reading the docs
// (CLAUDE.md list-endpoint convention).
type PaginationMeta struct {
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	Total   int  `json:"total"`
	HasMore bool `json:"has_more"`
}

// PaginatedEventsResponse is the wire shape for GET /api/v1/events.
type PaginatedEventsResponse struct {
	Data       []EventResponse `json:"data"`
	Pagination PaginationMeta  `json:"pagination"`
}

// PaginatedSeatsResponse is the wire shape for GET /api/v1/events/{eventID}/seats.
type PaginatedSeatsResponse struct {
	Data       []SeatResponse `json:"data"`
	Pagination PaginationMeta `json:"pagination"`
}

// SectionRequest is one rectangular block of seats in a layout: Rows numbered
// "1".."Rows", each with SeatsPerRow seats, all at PriceMinor.
type SectionRequest struct {
	Name        string `json:"name"`
	Rows        int    `json:"rows"`
	SeatsPerRow int    `json:"seats_per_row"`
	PriceMinor  int64  `json:"price_minor"`
}

// ExceptionRequest adjusts one generated seat: remove it, override its price, or
// both. Address it with the same string row/number the grid produces.
type ExceptionRequest struct {
	Section    string `json:"section"`
	Row        string `json:"row"`
	Number     string `json:"number"`
	Remove     bool   `json:"remove"`
	PriceMinor *int64 `json:"price_minor,omitempty"`
}

// LayoutRequest is the compact seat-map description: a few rectangular sections
// plus per-seat exceptions for the irregular parts. The service expands it into
// individual seats.
type LayoutRequest struct {
	Sections   []SectionRequest   `json:"sections"`
	Exceptions []ExceptionRequest `json:"exceptions"`
}

// CreateEventRequest is the body of POST /api/v1/events. Every event is created
// together with its seat map, described as a layout rather than an explicit list.
type CreateEventRequest struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Venue       string        `json:"venue"`
	StartsAt    time.Time     `json:"starts_at"`
	EndsAt      time.Time     `json:"ends_at"`
	Layout      LayoutRequest `json:"layout"`
}

// ToLayoutSpec converts the request's layout into the domain input type. This is
// transport decoding, so it lives at the HTTP edge, not in the usecase.
func (r CreateEventRequest) ToLayoutSpec() domain.LayoutSpec {
	sections := make([]domain.SectionSpec, 0, len(r.Layout.Sections))
	for _, s := range r.Layout.Sections {
		sections = append(sections, domain.SectionSpec{
			Name:        s.Name,
			Rows:        s.Rows,
			SeatsPerRow: s.SeatsPerRow,
			PriceMinor:  s.PriceMinor,
		})
	}
	exceptions := make([]domain.SeatException, 0, len(r.Layout.Exceptions))
	for _, e := range r.Layout.Exceptions {
		exceptions = append(exceptions, domain.SeatException{
			Section:    e.Section,
			Row:        e.Row,
			Number:     e.Number,
			Remove:     e.Remove,
			PriceMinor: e.PriceMinor,
		})
	}
	return domain.LayoutSpec{Sections: sections, Exceptions: exceptions}
}

// CreateEventResponse is the wire shape for a freshly created event. The seat
// map is read back (paginated) via GET /api/v1/events/{eventID}/seats rather
// than returned inline, so this response stays bounded no matter how many seats
// the event has.
type CreateEventResponse struct {
	Event     EventResponse `json:"event"`
	SeatCount int           `json:"seat_count"`
}

// ToCreateEventResponse is the one domain -> wire mapper for a created event,
// kept next to the DTO (CLAUDE.md convention).
func ToCreateEventResponse(e domain.Event, seats []domain.Seat) CreateEventResponse {
	return CreateEventResponse{
		Event:     ToEventResponse(e),
		SeatCount: len(seats),
	}
}

// ErrorResponse is the single error envelope every handler returns.
type ErrorResponse struct {
	Error string `json:"error"`
}

// ToEventResponse is the one domain -> wire mapper for an event, kept next to
// the DTO (CLAUDE.md convention) so the mapping lives in one place instead of
// being re-derived inline per handler.
func ToEventResponse(e domain.Event) EventResponse {
	return EventResponse{
		ID:          e.ID,
		Name:        e.Name,
		Description: e.Description,
		Venue:       e.Venue,
		StartsAt:    e.StartsAt,
		EndsAt:      e.EndsAt,
		CreatedAt:   e.CreatedAt,
	}
}

// ToSeatResponse is the one domain -> wire mapper for a seat.
func ToSeatResponse(s domain.Seat) SeatResponse {
	return SeatResponse{
		ID:         s.ID,
		EventID:    s.EventID,
		Section:    s.Section,
		Row:        s.Row,
		Number:     s.Number,
		Status:     s.Status,
		PriceMinor: s.PriceMinor,
	}
}

// toPaginationMeta builds the envelope meta from the validated Pagination and
// the total match count. Shared by every list mapper below.
func toPaginationMeta(p domain.Pagination, pageLen, total int) PaginationMeta {
	return PaginationMeta{
		Limit:   p.Limit,
		Offset:  p.Offset,
		Total:   total,
		HasMore: p.HasMore(pageLen, total),
	}
}

// ToPaginatedEventsResponse maps a page of events + its total into the list
// envelope (extends the response-mapper convention to the envelope, not just
// the single-item DTO).
func ToPaginatedEventsResponse(events []domain.Event, p domain.Pagination, total int) PaginatedEventsResponse {
	data := make([]EventResponse, 0, len(events))
	for _, e := range events {
		data = append(data, ToEventResponse(e))
	}
	return PaginatedEventsResponse{Data: data, Pagination: toPaginationMeta(p, len(data), total)}
}

// ToPaginatedSeatsResponse maps a page of seats + its total into the list
// envelope.
func ToPaginatedSeatsResponse(seats []domain.Seat, p domain.Pagination, total int) PaginatedSeatsResponse {
	data := make([]SeatResponse, 0, len(seats))
	for _, s := range seats {
		data = append(data, ToSeatResponse(s))
	}
	return PaginatedSeatsResponse{Data: data, Pagination: toPaginationMeta(p, len(data), total)}
}
