package http

import (
	"net/http"

	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(h *Handler, health *HealthHandler, mw ...Middleware) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", health.Live)
	mux.HandleFunc("GET /readyz", health.Ready)
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)

	mux.HandleFunc("GET /api/v1/analytics/events/{eventID}", h.GetEventStats)
	mux.HandleFunc("GET /api/v1/analytics/users/{userID}", h.GetUserRegistration)

	return Chain(mux, mw...)
}
