package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

// Handler holds the use cases the HTTP layer drives. It is constructed once at
// startup and shared across every request — no per-request mutable field.
type Handler struct {
	listEvents     *usecase.ListEventsUseCase
	getEvent       *usecase.GetEventUseCase
	listEventSeats *usecase.ListEventSeatsUseCase
}

func NewHandler(
	listEvents *usecase.ListEventsUseCase,
	getEvent *usecase.GetEventUseCase,
	listEventSeats *usecase.ListEventSeatsUseCase,
) *Handler {
	return &Handler{
		listEvents:     listEvents,
		getEvent:       getEvent,
		listEventSeats: listEventSeats,
	}
}

// ListEvents godoc
//
//	@Summary		List events
//	@Description	Browse events newest first. Optionally restrict to upcoming events. Paginated with limit/offset.
//	@Tags			events
//	@Produce		json
//	@Param			upcoming	query		bool	false	"only events that have not ended yet"
//	@Param			limit		query		int		false	"page size (default 20, max 100)"
//	@Param			offset		query		int		false	"rows to skip (default 0)"
//	@Success		200			{object}	PaginatedEventsResponse
//	@Failure		400			{object}	ErrorResponse	"invalid limit/offset"
//	@Failure		500			{object}	ErrorResponse	"internal server error"
//	@Router			/events [get]
func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	f := domain.EventFilter{UpcomingOnly: q.Get("upcoming") == "true"}

	p, ok := parsePagination(w, q)
	if !ok {
		return
	}

	events, total, err := h.listEvents.Execute(r.Context(), f, p)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ToPaginatedEventsResponse(events, p, total))
}

// GetEvent godoc
//
//	@Summary		Get one event
//	@Description	Fetch a single event by its UUID.
//	@Tags			events
//	@Produce		json
//	@Param			eventID	path		string	true	"event UUID"
//	@Success		200		{object}	EventResponse
//	@Failure		400		{object}	ErrorResponse	"eventID is not a UUID"
//	@Failure		404		{object}	ErrorResponse	"no such event"
//	@Failure		500		{object}	ErrorResponse	"internal server error"
//	@Router			/events/{eventID} [get]
func (h *Handler) GetEvent(w http.ResponseWriter, r *http.Request) {
	eventID, err := uuid.Parse(r.PathValue("eventID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "eventID must be a valid UUID")
		return
	}

	event, err := h.getEvent.Execute(r.Context(), eventID)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ToEventResponse(event))
}

// ListEventSeats godoc
//
//	@Summary		List an event's seats
//	@Description	The seat map for one event, ordered by section/row/number. Paginated with limit/offset.
//	@Tags			events
//	@Produce		json
//	@Param			eventID	path		string	true	"event UUID"
//	@Param			limit	query		int		false	"page size (default 20, max 100)"
//	@Param			offset	query		int		false	"rows to skip (default 0)"
//	@Success		200		{object}	PaginatedSeatsResponse
//	@Failure		400		{object}	ErrorResponse	"eventID is not a UUID, or invalid limit/offset"
//	@Failure		404		{object}	ErrorResponse	"no such event"
//	@Failure		500		{object}	ErrorResponse	"internal server error"
//	@Router			/events/{eventID}/seats [get]
func (h *Handler) ListEventSeats(w http.ResponseWriter, r *http.Request) {
	eventID, err := uuid.Parse(r.PathValue("eventID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "eventID must be a valid UUID")
		return
	}

	p, ok := parsePagination(w, r.URL.Query())
	if !ok {
		return
	}

	seats, total, err := h.listEventSeats.Execute(r.Context(), eventID, p)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ToPaginatedSeatsResponse(seats, p, total))
}

// parsePagination reads limit/offset from the query. An absent param takes the
// domain default; a present-but-non-integer or negative value is a 400 (written
// here) and ok is false. On success it returns an already-validated
// domain.Pagination (limit clamped to the max).
func parsePagination(w http.ResponseWriter, q map[string][]string) (domain.Pagination, bool) {
	limit := domain.DefaultLimit
	offset := 0

	if v := firstQ(q, "limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return domain.Pagination{}, false
		}
		limit = n
	}
	if v := firstQ(q, "offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return domain.Pagination{}, false
		}
		offset = n
	}

	p, err := domain.NewPagination(limit, offset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return domain.Pagination{}, false
	}
	return p, true
}

func firstQ(q map[string][]string, key string) string {
	if vs := q[key]; len(vs) > 0 {
		return vs[0]
	}
	return ""
}

// writeDomainError translates a domain / repository error into an HTTP status at
// the edge. Business rules never appear here — only transport translation.
func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrInvalidPagination),
		errors.Is(err, domain.ErrInvalidEvent),
		errors.Is(err, domain.ErrInvalidSeat):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}
