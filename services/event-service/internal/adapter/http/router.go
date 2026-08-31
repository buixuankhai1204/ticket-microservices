package http

import (
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"
)

func NewRouter(h *Handler, health *HealthHandler, mw ...Middleware) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", health.Live)
	mux.HandleFunc("GET /readyz", health.Ready)

	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)

	mux.HandleFunc("GET /api/v1/events", h.ListEvents)
	mux.HandleFunc("POST /api/v1/events", h.CreateEvent)
	mux.HandleFunc("GET /api/v1/events/{eventID}", h.GetEvent)
	mux.HandleFunc("GET /api/v1/events/{eventID}/seats", h.ListEventSeats)

	return Chain(mux, mw...)
}
