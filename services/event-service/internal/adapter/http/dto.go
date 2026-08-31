package http

import (
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

type EventResponse struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Venue       string    `json:"venue"`
	StartsAt    time.Time `json:"starts_at"`
	EndsAt      time.Time `json:"ends_at"`
	CreatedAt   time.Time `json:"created_at"`
}

type SeatResponse struct {
	ID         uuid.UUID `json:"id"`
	EventID    uuid.UUID `json:"event_id"`
	Section    string    `json:"section"`
	Row        string    `json:"row"`
	Number     string    `json:"number"`
	Status     string    `json:"status"`
	PriceMinor int64     `json:"price_minor"`
}

type PaginationMeta struct {
	Limit   int  `json:"limit"`
	Offset  int  `json:"offset"`
	Total   int  `json:"total"`
	HasMore bool `json:"has_more"`
}

type PaginatedEventsResponse struct {
	Data       []EventResponse `json:"data"`
	Pagination PaginationMeta  `json:"pagination"`
}

type PaginatedSeatsResponse struct {
	Data       []SeatResponse `json:"data"`
	Pagination PaginationMeta `json:"pagination"`
}

type SectionRequest struct {
	Name        string `json:"name"`
	Rows        int    `json:"rows"`
	SeatsPerRow int    `json:"seats_per_row"`
	PriceMinor  int64  `json:"price_minor"`
}

type ExceptionRequest struct {
	Section    string `json:"section"`
	Row        string `json:"row"`
	Number     string `json:"number"`
	Remove     bool   `json:"remove"`
	PriceMinor *int64 `json:"price_minor,omitempty"`
}

type LayoutRequest struct {
	Sections   []SectionRequest   `json:"sections"`
	Exceptions []ExceptionRequest `json:"exceptions"`
}

type CreateEventRequest struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Venue       string        `json:"venue"`
	StartsAt    time.Time     `json:"starts_at"`
	EndsAt      time.Time     `json:"ends_at"`
	Layout      LayoutRequest `json:"layout"`
}

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

type CreateEventResponse struct {
	Event     EventResponse `json:"event"`
	SeatCount int           `json:"seat_count"`
}

func ToCreateEventResponse(e domain.Event, seats []domain.Seat) CreateEventResponse {
	return CreateEventResponse{
		Event:     ToEventResponse(e),
		SeatCount: len(seats),
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

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

func toPaginationMeta(p domain.Pagination, pageLen, total int) PaginationMeta {
	return PaginationMeta{
		Limit:   p.Limit,
		Offset:  p.Offset,
		Total:   total,
		HasMore: p.HasMore(pageLen, total),
	}
}

func ToPaginatedEventsResponse(events []domain.Event, p domain.Pagination, total int) PaginatedEventsResponse {
	data := make([]EventResponse, 0, len(events))
	for _, e := range events {
		data = append(data, ToEventResponse(e))
	}
	return PaginatedEventsResponse{Data: data, Pagination: toPaginationMeta(p, len(data), total)}
}

func ToPaginatedSeatsResponse(seats []domain.Seat, p domain.Pagination, total int) PaginatedSeatsResponse {
	data := make([]SeatResponse, 0, len(seats))
	for _, s := range seats {
		data = append(data, ToSeatResponse(s))
	}
	return PaginatedSeatsResponse{Data: data, Pagination: toPaginationMeta(p, len(data), total)}
}
