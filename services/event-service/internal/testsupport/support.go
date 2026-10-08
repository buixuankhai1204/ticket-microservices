package testsupport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
)

type SilentLogger struct{}

func (SilentLogger) Info(string, ...any)  {}
func (SilentLogger) Warn(string, ...any)  {}
func (SilentLogger) Error(string, ...any) {}
func (l SilentLogger) With(...any) logger.Logger {
	return l
}

func Eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

var (
	cleanupMu sync.Mutex
	cleanups  []func(context.Context)
)

func onShutdown(fn func(context.Context)) {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	cleanups = append(cleanups, fn)
}

func Run(m *testing.M) int {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	for _, fn := range cleanups {
		fn(ctx)
	}
	return code
}
