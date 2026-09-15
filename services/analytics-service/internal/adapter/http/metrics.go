package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var uninstrumentedPatterns = map[string]bool{
	"GET /healthz": true,
	"GET /readyz":  true,
	"GET /metrics": true,
}

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed, labeled by method, route, and status code.",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestsDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_requests_duration_seconds",
			Help:    "HTTP request latency in seconds, labeled by method, route, and status code.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)
)

func Metrics() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			pattern := r.Pattern
			if pattern == "" || uninstrumentedPatterns[pattern] {
				return
			}

			status := strconv.Itoa(rec.status)
			httpRequestsTotal.WithLabelValues(r.Method, patternPath(pattern), status).Inc()
			httpRequestsDuration.WithLabelValues(r.Method, patternPath(pattern), status).
				Observe(time.Since(start).Seconds())
		})
	}
}

// patternPath strips the leading "METHOD " that net/http.ServeMux prepends to
// registered patterns (e.g. "GET /api/v1/events/{eventID}"), leaving just the
// path template as a low-cardinality label value.
func patternPath(pattern string) string {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == ' ' {
			return pattern[i+1:]
		}
	}
	return pattern
}
