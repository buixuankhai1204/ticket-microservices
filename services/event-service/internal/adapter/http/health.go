package http

import (
	"context"
	"net/http"
	"time"
)

// Pinger is the minimal readiness dependency: something that can round-trip to
// the database. *pgxpool.Pool satisfies it. Kept as an interface here so
// readiness is an infra concern the HTTP layer owns, not a business use case.
type Pinger interface {
	Ping(ctx context.Context) error
}

// HealthHandler serves the liveness and readiness probes Kong / the orchestrator
// need for safe rolling deploys.
type HealthHandler struct {
	db Pinger
}

func NewHealthHandler(db Pinger) *HealthHandler {
	return &HealthHandler{db: db}
}

// Live reports process liveness without touching any dependency.
func (h *HealthHandler) Live(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Ready reports readiness, pinging the DB pool with a short bounded timeout.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}
