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
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/testsupport"
)

var binary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "event-service-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binary = filepath.Join(dir, "event-service")
	build := exec.Command("go", "build", "-o", binary, "./cmd")
	build.Dir = ".."
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build event-service: %v\n%s", err, out)
		os.Exit(1)
	}
	code := testsupport.Run(m)
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type process struct {
	cmd   *exec.Cmd
	base  string
	out   *bytes.Buffer
	done  chan error
	dbURL string
	topic string
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

func start(t *testing.T, extraEnv ...string) *process {
	t.Helper()
	port := freePort(t)
	topic := testsupport.NewTopic(t, "oop-booking-events")
	dbURL := testsupport.EmptyDatabaseURL(t)
	env := append(baseEnv(),
		fmt.Sprintf("PORT=%d", port),
		"DATABASE_URL="+dbURL,
		"KAFKA_BROKERS="+strings.Join(testsupport.Brokers(t), ","),
		"KAFKA_BOOKING_EVENTS_TOPIC="+topic,
		"KAFKA_GROUP_SUFFIX=-"+uuid.NewString()[:8],
		"KAFKA_CONSUMER_MAX_ATTEMPTS=3",
	)
	env = append(env, extraEnv...)
	p := &process{base: fmt.Sprintf("http://127.0.0.1:%d", port), out: &bytes.Buffer{}, done: make(chan error, 1), dbURL: dbURL, topic: topic}
	p.cmd = exec.Command(binary)
	p.cmd.Env = env
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start event-service: %v", err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
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

func (p *process) pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), p.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func (p *process) do(method, path string, body any) (int, []byte) {
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, p.base+path, reader)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, out
}

type seat struct {
	ID     uuid.UUID `json:"id"`
	Status string    `json:"status"`
}

func (p *process) createEvent(t *testing.T, seats int) (uuid.UUID, []seat) {
	t.Helper()
	status, body := p.do(http.MethodPost, "/api/v1/events", map[string]any{
		"name": "Fest", "venue": "Park", "starts_at": "2030-06-01T19:00:00Z", "ends_at": "2030-06-01T22:00:00Z",
		"layout": map[string]any{"sections": []map[string]any{{"name": "A", "rows": 1, "seats_per_row": seats, "price_minor": 5000}}},
	})
	if status != http.StatusCreated {
		t.Fatalf("create event = %d %s", status, body)
	}
	var created struct {
		Event struct {
			ID uuid.UUID `json:"id"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	return created.Event.ID, p.seats(t, created.Event.ID)
}

func (p *process) seats(t *testing.T, eventID uuid.UUID) []seat {
	t.Helper()
	status, body := p.do(http.MethodGet, "/api/v1/events/"+eventID.String()+"/seats?limit=100", nil)
	if status != http.StatusOK {
		t.Fatalf("list seats = %d %s", status, body)
	}
	var page struct {
		Data []seat `json:"data"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	return page.Data
}

func (p *process) waitSeat(t *testing.T, eventID, seatID uuid.UUID, want string) {
	t.Helper()
	testsupport.Eventually(t, 60*time.Second, "seat "+seatID.String()+" to be "+want, func() bool {
		for _, s := range p.seats(t, eventID) {
			if s.ID == seatID {
				return s.Status == want
			}
		}
		return false
	})
}

func bookingJSON(bookingID, eventID uuid.UUID, seatIDs ...uuid.UUID) []byte {
	ids := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		ids[i] = id.String()
	}
	now := time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]any{
		"event_id": uuid.NewString(), "booking_id": bookingID.String(), "user_id": uuid.NewString(),
		"ticketed_event_id": eventID.String(), "seat_ids": ids, "requested_at": now, "occurred_at": now,
	})
	return raw
}

func TestTheBinaryReservesAndBooksSeatsFromKafkaAndAnnouncesTheOutcome(t *testing.T) {
	t.Parallel()
	p := start(t)
	tap := testsupport.TapOutbox(t, p.pool(t))
	eventID, seats := p.createEvent(t, 2)
	bookingID := uuid.New()

	testsupport.Produce(t, p.topic, bookingID.String(), bookingJSON(bookingID, eventID, seats[0].ID), testsupport.EventType("BookingRequested"))
	p.waitSeat(t, eventID, seats[0].ID, "reserved")
	testsupport.Produce(t, p.topic, bookingID.String(), bookingJSON(bookingID, eventID, seats[0].ID), testsupport.EventType("BookingConfirmed"))
	p.waitSeat(t, eventID, seats[0].ID, "booked")

	if types := tap.Types(); len(types) != 1 || types[0] != "SeatReserved" {
		t.Fatalf("published = %v, want exactly one SeatReserved", types)
	}
}

func TestTheBinaryAnswersAConflictingRequestWithAFailureEvent(t *testing.T) {
	t.Parallel()
	p := start(t)
	tap := testsupport.TapOutbox(t, p.pool(t))
	eventID, seats := p.createEvent(t, 1)
	first, second := uuid.New(), uuid.New()

	testsupport.Produce(t, p.topic, first.String(), bookingJSON(first, eventID, seats[0].ID), testsupport.EventType("BookingRequested"))
	p.waitSeat(t, eventID, seats[0].ID, "reserved")
	testsupport.Produce(t, p.topic, second.String(), bookingJSON(second, eventID, seats[0].ID), testsupport.EventType("BookingRequested"))

	testsupport.Eventually(t, 60*time.Second, "the conflicting request to be answered", func() bool {
		return len(tap.Events()) == 2
	})
	failed := tap.Events()[1]
	if failed.EventType != "SeatReservationFailed" || failed.AggregateID != second || failed.Payload["reason"] != "seat_unavailable" {
		t.Fatalf("second outcome = %+v", failed)
	}
}

func TestTheBinaryDeadLettersAPoisonMessageAndKeepsServing(t *testing.T) {
	t.Parallel()
	p := start(t)
	eventID, seats := p.createEvent(t, 1)
	bookingID := uuid.New()

	testsupport.Produce(t, p.topic, "poison", []byte("{{ nope"), testsupport.EventType("BookingRequested"))
	testsupport.Produce(t, p.topic, bookingID.String(), bookingJSON(bookingID, eventID, seats[0].ID), testsupport.EventType("BookingRequested"))

	p.waitSeat(t, eventID, seats[0].ID, "reserved")
	dead := testsupport.ReadAll(t, p.topic+".dlq")
	if len(dead) != 1 || string(dead[0].Key) != "poison" || !strings.HasPrefix(testsupport.Header(dead[0], "x-dlq-reason"), "parse:") {
		t.Fatalf("dlq = %v, want exactly the poison message with a parse: reason", dead)
	}
}

func TestTheBinaryRunsTheReaperAndReleasesAHoldThatOutlivedTheTimeout(t *testing.T) {
	t.Parallel()
	p := start(t, "SEAT_REAPER_INTERVAL=1", "SEAT_HOLD_TIMEOUT=60")
	eventID, seats := p.createEvent(t, 1)
	bookingID := uuid.New()
	testsupport.Produce(t, p.topic, bookingID.String(), bookingJSON(bookingID, eventID, seats[0].ID), testsupport.EventType("BookingRequested"))
	p.waitSeat(t, eventID, seats[0].ID, "reserved")

	if _, err := p.pool(t).Exec(context.Background(),
		`UPDATE seat_reservations SET created_at = now() - interval '1 hour' WHERE booking_id = $1`, bookingID); err != nil {
		t.Fatal(err)
	}

	p.waitSeat(t, eventID, seats[0].ID, "available")
}

func TestTheBinaryAppliesItsMigrationsWhenItStarts(t *testing.T) {
	t.Parallel()
	p := start(t)
	pool := p.pool(t)

	for _, table := range []string{"events", "seats", "seat_reservations", "outbox_events", "processed_events"} {
		var found *string
		if err := pool.QueryRow(context.Background(), "SELECT to_regclass($1)::text", table).Scan(&found); err != nil || found == nil {
			t.Fatalf("table %s missing after startup (err %v)", table, err)
		}
	}
}

func TestTheBinaryStopsCleanlyOnSIGTERM(t *testing.T) {
	t.Parallel()
	p := start(t)

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("exit = %v, want a clean 0 after SIGTERM:\n%s", err, p.out.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("service did not stop within the shutdown grace:\n%s", p.out.String())
	}
}

func TestTheBinaryRefusesToStartWithoutItsRequiredConfiguration(t *testing.T) {
	t.Parallel()
	const dbURL = "DATABASE_URL=postgres://u:p@localhost:1/db"
	tests := []struct {
		name    string
		env     []string
		mention string
	}{
		{"no database url", []string{"KAFKA_BROKERS=localhost:1"}, "DATABASE_URL"},
		{"no kafka brokers", []string{dbURL}, "KAFKA_BROKERS"},
		{"port that is not a number", []string{"PORT=abc", dbURL, "KAFKA_BROKERS=localhost:1"}, "PORT"},
		{"hold timeout of zero", []string{"SEAT_HOLD_TIMEOUT=0", dbURL, "KAFKA_BROKERS=localhost:1"}, "SEAT_HOLD_TIMEOUT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(binary)
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
