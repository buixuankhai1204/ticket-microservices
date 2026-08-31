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

type Handler struct {
	listEvents     *usecase.ListEventsUseCase
	getEvent       *usecase.GetEventUseCase
	listEventSeats *usecase.ListEventSeatsUseCase
	createNewEvent *usecase.CreateNewEventUseCase
}

func NewHandler(
	listEvents *usecase.ListEventsUseCase,
	getEvent *usecase.GetEventUseCase,
	listEventSeats *usecase.ListEventSeatsUseCase,
	createNewEvent *usecase.CreateNewEventUseCase,
) *Handler {
	return &Handler{
		listEvents:     listEvents,
		getEvent:       getEvent,
		listEventSeats: listEventSeats,
		createNewEvent: createNewEvent,
	}
}

// @Summary		List events
// @Description	Browse events newest first. Optionally restrict to upcoming events. Paginated with limit/offset.
// @Tags			events
// @Produce		json
// @Param			upcoming	query		bool	false	"only events that have not ended yet"
// @Param			limit		query		int		false	"page size (default 20, max 100)"
// @Param			offset		query		int		false	"rows to skip (default 0)"
// @Success		200			{object}	PaginatedEventsResponse
// @Failure		400			{object}	ErrorResponse	"invalid limit/offset"
// @Failure		500			{object}	ErrorResponse	"internal server error"
// @Router			/events [get]
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

// @Summary		Create an event with its seat map
// @Description	Creates a new event and expands the given seat layout (rectangular sections + per-seat exceptions) into its seat map, all in one transaction. A 100k-seat venue is a ~1KB body. The seat map is read back (paginated) via GET /api/v1/events/{eventID}/seats; this response carries only the seat count.
// @Tags			events
// @Accept			json
// @Produce		json
// @Param			request	body		CreateEventRequest	true	"event fields plus the seat layout to expand"
// @Success		201		{object}	CreateEventResponse
// @Failure		400		{object}	ErrorResponse	"malformed body, invalid event, invalid or too-large layout, or no seats"
// @Failure		500		{object}	ErrorResponse	"internal server error"
// @Router			/events [post]
func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var req CreateEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON")
		return
	}

	event, seats, err := h.createNewEvent.Execute(r.Context(), usecase.CreateNewEventInput{
		Name:        req.Name,
		Description: req.Description,
		Venue:       req.Venue,
		StartsAt:    req.StartsAt,
		EndsAt:      req.EndsAt,
		Layout:      req.ToLayoutSpec(),
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, ToCreateEventResponse(event, seats))
}

// @Summary		Get one event
// @Description	Fetch a single event by its UUID.
// @Tags			events
// @Produce		json
// @Param			eventID	path		string	true	"event UUID"
// @Success		200		{object}	EventResponse
// @Failure		400		{object}	ErrorResponse	"eventID is not a UUID"
// @Failure		404		{object}	ErrorResponse	"no such event"
// @Failure		500		{object}	ErrorResponse	"internal server error"
// @Router			/events/{eventID} [get]
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

// @Summary		List an event's seats
// @Description	The seat map for one event, ordered by section/row/number. Paginated with limit/offset.
// @Tags			events
// @Produce		json
// @Param			eventID	path		string	true	"event UUID"
// @Param			limit	query		int		false	"page size (default 20, max 100)"
// @Param			offset	query		int		false	"rows to skip (default 0)"
// @Success		200		{object}	PaginatedSeatsResponse
// @Failure		400		{object}	ErrorResponse	"eventID is not a UUID, or invalid limit/offset"
// @Failure		404		{object}	ErrorResponse	"no such event"
// @Failure		500		{object}	ErrorResponse	"internal server error"
// @Router			/events/{eventID}/seats [get]
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

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrInvalidPagination),
		errors.Is(err, domain.ErrInvalidEvent),
		errors.Is(err, domain.ErrInvalidSeat),
		errors.Is(err, domain.ErrInvalidLayout),
		errors.Is(err, domain.ErrLayoutTooLarge),
		errors.Is(err, domain.ErrEventRequiresSeats):
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
