//go:build component

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/tests/common"
)

var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

func binary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "analytics-service-bin")
		if err != nil {
			binaryErr = err
			return
		}
		common.OnShutdown(func(context.Context) { _ = os.RemoveAll(dir) })
		binaryPath = filepath.Join(dir, "analytics-service")
		build := exec.Command("go", "build", "-o", binaryPath, "./cmd")
		build.Dir = ".."
		if out, err := build.CombinedOutput(); err != nil {
			binaryErr = fmt.Errorf("go build ./cmd: %w\n%s", err, out)
		}
	})
	if binaryErr != nil {
		t.Fatalf("build analytics-service: %v", binaryErr)
	}
	return binaryPath
}

type topics struct {
	user, booking string
}

func newTopics(t *testing.T) topics {
	t.Helper()
	return topics{
		user:    common.NewTopic(t, "oop-user-events"),
		booking: common.NewTopic(t, "oop-booking-events"),
	}
}

type process struct {
	cmd    *exec.Cmd
	base   string
	out    *bytes.Buffer
	done   chan error
	exited chan struct{}
	dbURL  string
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func baseEnv() []string {
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
}

func startOnKafka(t *testing.T, tp topics) *process {
	t.Helper()
	port := freePort(t)
	dbURL := common.EmptyDatabaseURL(t)
	env := append(baseEnv(),
		fmt.Sprintf("PORT=%d", port),
		"DATABASE_URL="+dbURL,
		"KAFKA_BROKERS="+strings.Join(common.Brokers(t), ","),
		"KAFKA_USER_EVENTS_TOPIC="+tp.user,
		"KAFKA_BOOKING_EVENTS_TOPIC="+tp.booking,
		"KAFKA_CONSUMER_MAX_ATTEMPTS=3",
	)
	p := &process{base: fmt.Sprintf("http://127.0.0.1:%d", port), out: &bytes.Buffer{}, done: make(chan error, 1), exited: make(chan struct{}), dbURL: dbURL}
	p.cmd = exec.Command(binary(t))
	p.cmd.Env = env
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start analytics-service: %v", err)
	}
	go func() {
		p.done <- p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.exited:
		case <-time.After(20 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.exited
		}
	})
	p.waitUntilReady(t)
	return p
}

func (p *process) waitUntilReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.done:
			t.Fatalf("service exited early (%v):\n%s", err, p.out.String())
		default:
		}
		if res, err := http.Get(p.base + "/readyz"); err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("service never became ready:\n%s", p.out.String())
}

func (p *process) get(path string) (int, []byte) {
	res, err := http.Get(p.base + path)
	if err != nil {
		return 0, nil
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, body
}

func userCreatedJSON(userID uuid.UUID, email string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"event_id":   uuid.NewString(),
		"user_id":    userID.String(),
		"email":      email,
		"created_at": "2026-03-04T08:30:00Z",
	})
	return raw
}

func outcomeJSON(bookingID, ticketedEventID uuid.UUID) []byte {
	raw, _ := json.Marshal(map[string]any{
		"event_id":          uuid.NewString(),
		"booking_id":        bookingID.String(),
		"ticketed_event_id": ticketedEventID.String(),
		"occurred_at":       "2026-03-04T08:30:00Z",
	})
	return raw
}

func TestTheBinaryTurnsKafkaEventsIntoTheHTTPReadModel(t *testing.T) {
	tp := newTopics(t)
	p := startOnKafka(t, tp)
	userID, ticketedEvent := uuid.New(), uuid.New()

	common.Produce(t, tp.user, userID.String(), userCreatedJSON(userID, "ada@example.com"), common.EventType("UserCreated"))
	common.Produce(t, tp.booking, uuid.NewString(), outcomeJSON(uuid.New(), ticketedEvent), common.EventType("BookingConfirmed"))
	common.Produce(t, tp.booking, uuid.NewString(), outcomeJSON(uuid.New(), ticketedEvent), common.EventType("BookingCancelled"))

	common.Eventually(t, 60*time.Second, "the registered user to be queryable", func() bool {
		status, body := p.get("/api/v1/analytics/users/" + userID.String())
		return status == http.StatusOK && strings.Contains(string(body), "ada@example.com")
	})
	common.Eventually(t, 60*time.Second, "the booking stats to show one confirmed and one cancelled", func() bool {
		status, body := p.get("/api/v1/analytics/events/" + ticketedEvent.String())
		var s struct{ Confirmed, Cancelled int }
		return status == http.StatusOK && json.Unmarshal(body, &s) == nil && s.Confirmed == 1 && s.Cancelled == 1
	})
}

func TestTheBinaryDeadLettersAPoisonMessageAndKeepsServing(t *testing.T) {
	tp := newTopics(t)
	p := startOnKafka(t, tp)
	userID := uuid.New()

	common.Produce(t, tp.user, "poison", []byte("{{ nope"), common.EventType("UserCreated"))
	common.Produce(t, tp.user, userID.String(), userCreatedJSON(userID, "grace@example.com"), common.EventType("UserCreated"))

	common.Eventually(t, 60*time.Second, "the good message behind the poison one to be applied", func() bool {
		status, _ := p.get("/api/v1/analytics/users/" + userID.String())
		return status == http.StatusOK
	})
	dead := common.ReadAll(t, tp.user+".dlq")
	if len(dead) != 1 || string(dead[0].Key) != "poison" || !strings.HasPrefix(common.Header(dead[0], "x-dlq-reason"), "parse:") {
		t.Fatalf("dlq = %v, want exactly the poison message with a parse: reason", dead)
	}
}

func TestTheBinaryRefusesToStartWithoutItsRequiredConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		env     []string
		mention string
	}{
		{"no database url", []string{"KAFKA_BROKERS=localhost:1"}, "DATABASE_URL"},
		{"no kafka brokers", []string{"DATABASE_URL=postgres://u:p@localhost:1/db"}, "KAFKA_BROKERS"},
		{"port that is not a number", []string{"PORT=abc", "DATABASE_URL=postgres://u:p@localhost:1/db", "KAFKA_BROKERS=localhost:1"}, "PORT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(binary(t))
			cmd.Env = append(baseEnv(), tc.env...)
			done := make(chan struct{})
			var out []byte
			var err error
			go func() {
				out, err = cmd.CombinedOutput()
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(20 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("service kept running with a bad configuration")
			}

			if err == nil || !strings.Contains(string(out), tc.mention) {
				t.Fatalf("err = %v, output %q: want a non-zero exit that names %s", err, out, tc.mention)
			}
		})
	}
}
