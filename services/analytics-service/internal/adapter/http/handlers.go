package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

// Handler holds the use cases the HTTP layer drives. It is constructed once at
// startup and shared across every request — no per-request mutable field.
type Handler struct {
	getEventStats       *usecase.GetEventStatsUseCase
	getUserRegistration *usecase.GetUserRegistrationUseCase
}

func NewHandler(
	getEventStats *usecase.GetEventStatsUseCase,
	getUserRegistration *usecase.GetUserRegistrationUseCase,
) *Handler {
	return &Handler{
		getEventStats:       getEventStats,
		getUserRegistration: getUserRegistration,
	}
}

// GetEventStats godoc
//
//	@Summary		Event booking stats
//	@Description	Confirmed / cancelled booking counts for one event, projected from booking-outcome saga events.
//	@Tags			analytics
//	@Produce		json
//	@Security		BearerAuth
//	@Param			eventID	path		string	true	"event UUID"
//	@Success		200		{object}	EventStatsResponse
//	@Failure		400		{object}	ErrorResponse	"eventID is not a UUID"
//	@Failure		404		{object}	ErrorResponse	"no stats for this event"
//	@Failure		500		{object}	ErrorResponse	"internal server error"
//	@Router			/analytics/events/{eventID} [get]
func (h *Handler) GetEventStats(w http.ResponseWriter, r *http.Request) {
	eventID, err := uuid.Parse(r.PathValue("eventID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "eventID must be a valid UUID")
		return
	}

	stats, err := h.getEventStats.Execute(r.Context(), eventID)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ToEventStatsResponse(stats))
}

// GetUserRegistration godoc
//
//	@Summary		User registration projection
//	@Description	The registration record projected from the UserCreated event user-service published.
//	@Tags			analytics
//	@Produce		json
//	@Security		BearerAuth
//	@Param			userID	path		string	true	"user UUID"
//	@Success		200		{object}	UserRegistrationResponse
//	@Failure		400		{object}	ErrorResponse	"userID is not a UUID"
//	@Failure		404		{object}	ErrorResponse	"no registration for this user"
//	@Failure		500		{object}	ErrorResponse	"internal server error"
//	@Router			/analytics/users/{userID} [get]
func (h *Handler) GetUserRegistration(w http.ResponseWriter, r *http.Request) {
	userID, err := uuid.Parse(r.PathValue("userID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "userID must be a valid UUID")
		return
	}

	reg, err := h.getUserRegistration.Execute(r.Context(), userID)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, ToUserRegistrationResponse(reg))
}

// writeDomainError translates a domain / repository error into an HTTP status at
// the edge. Business rules never appear here — only transport translation.
func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrInvalidOutcomeStatus):
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
