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
	getEventStats *usecase.GetEventStatsUseCase
}

func NewHandler(getEventStats *usecase.GetEventStatsUseCase) *Handler {
	return &Handler{getEventStats: getEventStats}
}

// GetEventStats handles GET /api/v1/analytics/events/{eventID}.
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
