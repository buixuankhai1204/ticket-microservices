package http

import "net/http"

// NewRouter wires every route for the service. Prefixes must match
// kong/kong.yml exactly: Kong routes this service with strip_path: false, so it
// receives the full /api/v1/analytics path and must NOT strip the prefix.
func NewRouter(h *Handler, health *HealthHandler, mw ...Middleware) http.Handler {
	mux := http.NewServeMux()

	// Infra probes (not under the gateway prefix, not rate-limited).
	mux.HandleFunc("GET /healthz", health.Live)
	mux.HandleFunc("GET /readyz", health.Ready)

	// Client-facing routes, full /api/v1/analytics prefix retained.
	mux.HandleFunc("GET /api/v1/analytics/events/{eventID}", h.GetEventStats)
	mux.HandleFunc("GET /api/v1/analytics/users/{userID}", h.GetUserRegistration)

	return Chain(mux, mw...)
}
