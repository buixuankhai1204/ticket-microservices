//go:build e2e

// Tests for docs/sagas/seat-reservation.md, driven end-to-end through Kong
// (never a service's own port) against a docker-compose stack the caller has
// already brought up. e2e-saga-tester already drove this saga by hand and
// confirmed both the happy path and the oversell/compensation path pass
// cleanly against this implementation (see that session's report); this file
// turns that one-off verification into a suite that keeps proving it.
//
// §5 failure sequences judged unreachable from this black-box tier, and why:
//   - 5.2/5.3 (event-service or booking-service "down", the reaper picking up
//     a stale pending/held row) needs either killing a container or waiting
//     out PENDING_TIMEOUT/HOLD_TIMEOUT (2min/30min) with nothing consuming --
//     neither is a client/operator action available through Kong or a plain
//     Kafka produce.
//   - 5.4 (ConfirmBooking transient DB blip) and 5.6 (event-service restored
//     from backup missing a seat_reservations row) need injecting a database
//     fault or data loss -- no public trigger.
//   - 5.5 (FinalizeSeat fails because a seat row is anomalous) needs a seat
//     row mutated behind the public API -- there is no endpoint to do that,
//     and reaching around it with direct SQL would be exactly the "back door"
//     this tier's ground rules rule out.
//
// 5.1 (oversell/contention) and 5.7 (poison message) *are* reachable from
// outside and are covered below.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"

	"github.com/buixuankhai1204/ticket-microservice-golang/e2e/internal/harness"
)

// ---------------------------------------------------------------------------
// Fixtures: the saga's own HTTP surface, all through Kong.
// ---------------------------------------------------------------------------

type sectionSpec struct {
	Name        string `json:"name"`
	Rows        int    `json:"rows"`
	SeatsPerRow int    `json:"seats_per_row"`
	PriceMinor  int64  `json:"price_minor"`
}

type layoutSpec struct {
	Sections []sectionSpec `json:"sections"`
}

type createEventRequest struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Venue       string     `json:"venue"`
	StartsAt    time.Time  `json:"starts_at"`
	EndsAt      time.Time  `json:"ends_at"`
	Layout      layoutSpec `json:"layout"`
}

type eventDTO struct {
	ID string `json:"id"`
}

type createEventResponseDTO struct {
	Event     eventDTO `json:"event"`
	SeatCount int      `json:"seat_count"`
}

type seatDTO struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type paginatedSeatsDTO struct {
	Data []seatDTO `json:"data"`
}

// createEventWithSeats mints a fresh event (event-service's POST /events
// needs no JWT -- kong.yml's event-routes carries no jwt plugin) with exactly
// seatCount bookable seats, and returns the event id plus every seat id.
func createEventWithSeats(t *testing.T, ctx context.Context, seatCount int) (uuid.UUID, []uuid.UUID) {
	t.Helper()

	req := createEventRequest{
		Name:        "e2e fixture event " + t.Name(),
		Description: "seat-reservation e2e fixture, safe to leave behind",
		Venue:       "e2e venue",
		StartsAt:    time.Now().Add(48 * time.Hour).UTC(),
		EndsAt:      time.Now().Add(50 * time.Hour).UTC(),
		Layout: layoutSpec{
			Sections: []sectionSpec{{Name: "A", Rows: 1, SeatsPerRow: seatCount, PriceMinor: 1000}},
		},
	}
	var resp createEventResponseDTO
	status := harness.DoJSON(t, ctx, http.MethodPost, "/api/v1/events", "", req, &resp)
	if status != http.StatusCreated {
		t.Fatalf("create event: expected 201, got %d", status)
	}
	eventID, err := uuid.Parse(resp.Event.ID)
	if err != nil {
		t.Fatalf("parse event id %q: %v", resp.Event.ID, err)
	}

	var seats paginatedSeatsDTO
	status = harness.DoJSON(t, ctx, http.MethodGet, fmt.Sprintf("/api/v1/events/%s/seats?limit=100", eventID), "", nil, &seats)
	if status != http.StatusOK {
		t.Fatalf("list seats: expected 200, got %d", status)
	}
	if len(seats.Data) < seatCount {
		t.Fatalf("expected at least %d seats for event %s, got %d", seatCount, eventID, len(seats.Data))
	}

	seatIDs := make([]uuid.UUID, 0, seatCount)
	for _, s := range seats.Data[:seatCount] {
		id, err := uuid.Parse(s.ID)
		if err != nil {
			t.Fatalf("parse seat id %q: %v", s.ID, err)
		}
		seatIDs = append(seatIDs, id)
	}
	return eventID, seatIDs
}

type createBookingRequest struct {
	EventID uuid.UUID   `json:"event_id"`
	SeatIDs []uuid.UUID `json:"seat_ids"`
}

type bookingDTO struct {
	ID            string   `json:"id"`
	UserID        string   `json:"user_id"`
	EventID       string   `json:"event_id"`
	SeatIDs       []string `json:"seat_ids"`
	Status        string   `json:"status"`
	FailureReason *string  `json:"failure_reason"`
}

// createBooking is the saga's initiator step (docs/sagas/seat-reservation.md
// §2 step 1), asserting the documented 202 Accepted + status=pending contract
// from §7 ("What a polling client sees").
func createBooking(t *testing.T, ctx context.Context, token string, eventID uuid.UUID, seatIDs []uuid.UUID) bookingDTO {
	t.Helper()
	var resp bookingDTO
	status := harness.DoJSON(t, ctx, http.MethodPost, "/api/v1/bookings", token,
		createBookingRequest{EventID: eventID, SeatIDs: seatIDs}, &resp)
	if status != http.StatusAccepted {
		t.Fatalf("create booking: expected 202, got %d (body=%+v)", status, resp)
	}
	if resp.Status != "pending" {
		t.Fatalf("create booking: expected initial status pending, got %q", resp.Status)
	}
	return resp
}

func getBooking(t *testing.T, ctx context.Context, token, bookingID string) bookingDTO {
	t.Helper()
	var resp bookingDTO
	status := harness.DoJSON(t, ctx, http.MethodGet, "/api/v1/bookings/"+bookingID, token, nil, &resp)
	if status != http.StatusOK {
		t.Fatalf("get booking %s: expected 200, got %d", bookingID, status)
	}
	return resp
}

func waitForTerminalBooking(t *testing.T, ctx context.Context, token, bookingID string) bookingDTO {
	t.Helper()
	var final bookingDTO
	harness.PollUntil(t, 20*time.Second, 300*time.Millisecond, "booking "+bookingID+" to reach a terminal state", func() bool {
		final = getBooking(t, ctx, token, bookingID)
		return final.Status == "confirmed" || final.Status == "cancelled"
	})
	return final
}

// ---------------------------------------------------------------------------
// Fixtures: participant DB assertions (§4/§6 "Terminal:" rows).
// ---------------------------------------------------------------------------

func assertBookingRow(t *testing.T, ctx context.Context, bookingID uuid.UUID, wantStatus string, wantFailureReason *string) {
	t.Helper()
	var status string
	var failureReason *string
	err := harnessPools.Booking.QueryRow(ctx, `select status, failure_reason from bookings where id=$1`, bookingID).
		Scan(&status, &failureReason)
	if err != nil {
		t.Fatalf("query bookings row %s: %v", bookingID, err)
	}
	if status != wantStatus {
		t.Errorf("bookings.status[%s] = %q, want %q", bookingID, status, wantStatus)
	}
	switch {
	case wantFailureReason == nil && failureReason != nil:
		t.Errorf("bookings.failure_reason[%s] = %q, want NULL", bookingID, *failureReason)
	case wantFailureReason != nil && (failureReason == nil || *failureReason != *wantFailureReason):
		t.Errorf("bookings.failure_reason[%s] = %v, want %q", bookingID, failureReason, *wantFailureReason)
	}
}

// pollQueryString polls query (a single-string-column lookup, e.g. "select
// status from ... where id=$1") until it returns want, treating *any* error
// -- including pgx.ErrNoRows, expected while a fanned-out consumer hasn't
// caught up yet -- as "not there yet" rather than fatal. Every saga hop in
// this repo is sub-second in normal operation (CLAUDE.md), so a bounded wait
// here is a real assertion (a fan-out step that never lands is a genuine
// defect), not a sleep-to-avoid-flake.
func pollQueryString(t *testing.T, ctx context.Context, timeout time.Duration, describe string, query func(context.Context) (string, error), want string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	var lastErr error
	for {
		last, lastErr = query(ctx)
		if lastErr == nil && last == want {
			return
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("timed out after %s waiting for %s: last error: %v", timeout, describe, lastErr)
			}
			t.Fatalf("timed out after %s waiting for %s: last value %q, want %q", timeout, describe, last, want)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// assertSeatStatus polls: event-service's FinalizeSeat/ReleaseSeat run as a
// *second*, independent consumer group on booking.events, fanned out from the
// same BookingConfirmed/BookingCancelled event that already drove the
// initiator's own status to a terminal value -- so it can lag the initiator's
// GET response by up to a hop.
func assertSeatStatus(t *testing.T, ctx context.Context, seatID uuid.UUID, want string) {
	t.Helper()
	pollQueryString(t, ctx, 15*time.Second, fmt.Sprintf("seats.status[%s]", seatID),
		func(ctx context.Context) (string, error) {
			var status string
			err := harnessPools.Event.QueryRow(ctx, `select status from seats where id=$1`, seatID).Scan(&status)
			return status, err
		}, want)
}

func assertSeatReservationStatus(t *testing.T, ctx context.Context, bookingID uuid.UUID, want string) {
	t.Helper()
	pollQueryString(t, ctx, 15*time.Second, fmt.Sprintf("seat_reservations.status[%s]", bookingID),
		func(ctx context.Context) (string, error) {
			var status string
			err := harnessPools.Event.QueryRow(ctx, `select status from seat_reservations where booking_id=$1`, bookingID).Scan(&status)
			return status, err
		}, want)
}

func assertNoSeatReservationRow(t *testing.T, ctx context.Context, bookingID uuid.UUID) {
	t.Helper()
	var count int
	err := harnessPools.Event.QueryRow(ctx, `select count(*) from seat_reservations where booking_id=$1`, bookingID).Scan(&count)
	if err != nil {
		t.Fatalf("count seat_reservations for %s: %v", bookingID, err)
	}
	if count != 0 {
		t.Errorf("expected no seat_reservations row for booking %s (fail-before-reserve, §5.1), found %d", bookingID, count)
	}
}

// assertBookingOutcome polls: analytics-service's RecordBookingOutcome is a
// third, independent consumer group on booking.events (see assertSeatStatus).
func assertBookingOutcome(t *testing.T, ctx context.Context, bookingID uuid.UUID, want string) {
	t.Helper()
	pollQueryString(t, ctx, 15*time.Second, fmt.Sprintf("booking_outcomes.status[%s]", bookingID),
		func(ctx context.Context) (string, error) {
			var status string
			err := harnessPools.Analytics.QueryRow(ctx, `select status from booking_outcomes where booking_id=$1`, bookingID).Scan(&status)
			return status, err
		}, want)

	var count int
	if err := harnessPools.Analytics.QueryRow(ctx, `select count(*) from booking_outcomes where booking_id=$1`, bookingID).Scan(&count); err != nil {
		t.Fatalf("count booking_outcomes for %s: %v", bookingID, err)
	}
	if count != 1 {
		t.Errorf("expected exactly one booking_outcomes row for %s (UNIQUE(booking_id)), found %d", bookingID, count)
	}
}

// pollProcessedEventCount waits for a fanned-out consumer's dedupe row to
// appear (want=1), tolerating the same cross-service lag as assertSeatStatus.
func pollProcessedEventCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var count int
		err := pool.QueryRow(ctx, `select count(*) from processed_events where event_id=$1`, eventID).Scan(&count)
		if err == nil && count == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 15s waiting for processed_events count for event_id=%s to reach %d (err=%v)", eventID, want, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// assertProcessedEventCount is the instant, exact-count check used once a
// terminal state has already been independently established (e.g. after a
// deliberate settle wait, or right after confirming the message reached its
// own terminal DLQ outcome) -- unlike pollProcessedEventCount, a mismatch
// here means "changed when it must not have", not "hasn't landed yet".
func assertProcessedEventCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID uuid.UUID, want int) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `select count(*) from processed_events where event_id=$1`, eventID).Scan(&count); err != nil {
		t.Fatalf("count processed_events for %s: %v", eventID, err)
	}
	if count != want {
		t.Errorf("processed_events count for event_id=%s = %d, want %d", eventID, count, want)
	}
}

// ---------------------------------------------------------------------------
// 1. Happy path (docs/sagas/seat-reservation.md §4).
// ---------------------------------------------------------------------------

func TestSeatReservationHappyPath(t *testing.T) {
	requireStack(t)
	ctx := context.Background()

	dlqBooking := harness.NewDLQWatch(ctx, harness.DLQTopic(harness.BookingEventsTopic()))
	dlqSeatRes := harness.NewDLQWatch(ctx, harness.DLQTopic(harness.SeatReservationEventsTopic()))

	_, token := harness.RegisterAndLogin(t, ctx)
	eventID, seatIDs := createEventWithSeats(t, ctx, 1)
	created := createBooking(t, ctx, token, eventID, seatIDs)
	bookingID, err := uuid.Parse(created.ID)
	if err != nil {
		t.Fatalf("parse booking id: %v", err)
	}

	final := waitForTerminalBooking(t, ctx, token, created.ID)
	if final.Status != "confirmed" {
		t.Fatalf("expected booking to confirm, got status=%s failure_reason=%v", final.Status, final.FailureReason)
	}

	// §4 "Terminal:" row, across every participant, not just the initiator.
	assertBookingRow(t, ctx, bookingID, "confirmed", nil)
	for _, seatID := range seatIDs {
		assertSeatStatus(t, ctx, seatID, "booked")
	}
	assertSeatReservationStatus(t, ctx, bookingID, "finalized")
	assertBookingOutcome(t, ctx, bookingID, "confirmed")

	// §5 (standing assertion, not just the poison test in §5.7).
	dlqBooking.AssertEmpty(t, ctx, 4*time.Second)
	dlqSeatRes.AssertEmpty(t, ctx, 4*time.Second)
}

// ---------------------------------------------------------------------------
// 2. Oversell / contention -> compensation (§5.1, the one client-triggerable
// failure sequence in this saga).
// ---------------------------------------------------------------------------

func TestSeatReservationOversellCompensates(t *testing.T) {
	requireStack(t)
	ctx := context.Background()

	dlqBooking := harness.NewDLQWatch(ctx, harness.DLQTopic(harness.BookingEventsTopic()))
	dlqSeatRes := harness.NewDLQWatch(ctx, harness.DLQTopic(harness.SeatReservationEventsTopic()))

	_, tokenA := harness.RegisterAndLogin(t, ctx)
	_, tokenB := harness.RegisterAndLogin(t, ctx)
	eventID, seatIDs := createEventWithSeats(t, ctx, 1)
	seatID := seatIDs[0]

	first := createBooking(t, ctx, tokenA, eventID, []uuid.UUID{seatID})
	firstBookingID, err := uuid.Parse(first.ID)
	if err != nil {
		t.Fatalf("parse first booking id: %v", err)
	}
	firstFinal := waitForTerminalBooking(t, ctx, tokenA, first.ID)
	if firstFinal.Status != "confirmed" {
		t.Fatalf("setup: expected first booking to confirm before the contended one runs, got %s", firstFinal.Status)
	}

	// Second booking on the SAME seat, once it is already booked: this is the
	// oversell/contention case -- event-service's `FOR UPDATE` on the seats
	// row will see it non-available and emit SeatReservationFailed.
	second := createBooking(t, ctx, tokenB, eventID, []uuid.UUID{seatID})
	secondBookingID, err := uuid.Parse(second.ID)
	if err != nil {
		t.Fatalf("parse second booking id: %v", err)
	}
	secondFinal := waitForTerminalBooking(t, ctx, tokenB, second.ID)
	if secondFinal.Status != "cancelled" {
		t.Fatalf("expected the contended booking to be cancelled by compensation, got %s", secondFinal.Status)
	}
	if secondFinal.FailureReason == nil || *secondFinal.FailureReason != "seat_unavailable" {
		t.Fatalf("expected failure_reason=seat_unavailable, got %v", secondFinal.FailureReason)
	}

	// §5.1 "Terminal state": the compensated booking.
	assertBookingRow(t, ctx, secondBookingID, "cancelled", strPtr("seat_unavailable"))
	assertSeatStatus(t, ctx, seatID, "booked") // untouched -- still the first booking's, never touched by the failed one
	assertNoSeatReservationRow(t, ctx, secondBookingID)
	assertBookingOutcome(t, ctx, secondBookingID, "cancelled")

	// Nothing left non-terminal or orphaned on the winning booking either.
	assertBookingRow(t, ctx, firstBookingID, "confirmed", nil)
	assertSeatReservationStatus(t, ctx, firstBookingID, "finalized")
	assertBookingOutcome(t, ctx, firstBookingID, "confirmed")

	dlqBooking.AssertEmpty(t, ctx, 4*time.Second)
	dlqSeatRes.AssertEmpty(t, ctx, 4*time.Second)
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// 3. Idempotency: a duplicate BookingConfirmed redelivery must not double
// apply anywhere it fans out to (event-service's FinalizeSeat *and*
// analytics-service's RecordBookingOutcome), closing the gap e2e-saga-tester
// flagged (it could not force a live consumer group's redelivery without
// unsafely resetting a committed offset).
// ---------------------------------------------------------------------------

func TestSeatReservationIdempotentDuplicateBookingConfirmed(t *testing.T) {
	requireKafka(t)
	ctx := context.Background()

	_, token := harness.RegisterAndLogin(t, ctx)
	eventID, seatIDs := createEventWithSeats(t, ctx, 1)
	created := createBooking(t, ctx, token, eventID, seatIDs)
	bookingID, err := uuid.Parse(created.ID)
	if err != nil {
		t.Fatalf("parse booking id: %v", err)
	}

	final := waitForTerminalBooking(t, ctx, token, created.ID)
	if final.Status != "confirmed" {
		t.Fatalf("setup: expected booking to confirm, got %s", final.Status)
	}
	// Sanity: the happy path's own terminal state, before we duplicate anything.
	assertSeatStatus(t, ctx, seatIDs[0], "booked")
	assertSeatReservationStatus(t, ctx, bookingID, "finalized")
	assertBookingOutcome(t, ctx, bookingID, "confirmed")

	// Snoop the real BookingConfirmed message Debezium produced for this
	// booking -- byte-for-byte, this is what we redeliver.
	bookingEventsTopic := harness.BookingEventsTopic()
	original, err := harness.FindMessage(ctx, bookingEventsTopic, 20*time.Second,
		harness.MatchEventTypeAndBooking("BookingConfirmed", bookingID.String()))
	if err != nil {
		t.Fatalf("could not locate the real BookingConfirmed message on %s: %v", bookingEventsTopic, err)
	}
	eventIDHeader := harness.HeaderValue(original, "event_id")
	realEventID, err := uuid.Parse(eventIDHeader)
	if err != nil {
		t.Fatalf("real BookingConfirmed message has no valid event_id header (%q): %v", eventIDHeader, err)
	}
	// Both consumer groups already recorded it once, from the natural flow.
	assertProcessedEventCount(t, ctx, harnessPools.Event, realEventID, 1)
	assertProcessedEventCount(t, ctx, harnessPools.Analytics, realEventID, 1)

	dlqBooking := harness.NewDLQWatch(ctx, harness.DLQTopic(bookingEventsTopic))

	// This *is* what an uncommitted-offset Debezium redelivery looks like from
	// the consumer's side: the identical key, event_id, event_type header and
	// payload, produced again directly onto the topic.
	writer := harness.NewWriter(bookingEventsTopic)
	defer writer.Close()
	dup := kafka.Message{
		Key:   append([]byte(nil), original.Key...),
		Value: append([]byte(nil), original.Value...),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("BookingConfirmed")},
			{Key: "event_id", Value: []byte(eventIDHeader)},
		},
	}
	if err := writer.WriteMessages(ctx, dup); err != nil {
		t.Fatalf("produce duplicate BookingConfirmed: %v", err)
	}

	// Bounded window for both consumer groups to fetch, dedupe-check and skip
	// the duplicate -- there is no external signal to poll for a no-op, so we
	// wait long enough that a bug (a double-apply) would already have
	// happened, then assert nothing changed.
	time.Sleep(5 * time.Second)

	assertSeatStatus(t, ctx, seatIDs[0], "booked")
	assertSeatReservationStatus(t, ctx, bookingID, "finalized")
	assertBookingOutcome(t, ctx, bookingID, "confirmed")
	assertProcessedEventCount(t, ctx, harnessPools.Event, realEventID, 1)
	assertProcessedEventCount(t, ctx, harnessPools.Analytics, realEventID, 1)

	dlqBooking.AssertEmpty(t, ctx, 3*time.Second)
	harness.NewDLQWatch(ctx, harness.DLQTopic(harness.SeatReservationEventsTopic())).AssertEmpty(t, ctx, 3*time.Second)
}

// ---------------------------------------------------------------------------
// 4/5. Poison message per topic that has a DLQ in this saga (§5.7).
// ---------------------------------------------------------------------------

type bookingCancelledWire struct {
	EventID         string   `json:"event_id"`
	BookingID       string   `json:"booking_id"`
	UserID          string   `json:"user_id"`
	TicketedEventID string   `json:"ticketed_event_id"`
	SeatIDs         []string `json:"seat_ids"`
	Reason          string   `json:"reason"`
	OccurredAt      string   `json:"occurred_at"`
}

// assertDLQHeaders checks the x-dlq-* diagnostic headers every consumer
// engine in this repo writes (CLAUDE.md's dead-letter-handling section) and
// returns the original message's source partition/offset for further
// comparison.
func assertDLQHeaders(t *testing.T, m kafka.Message, wantSourceTopic, reasonPrefix string) (partition int, offset int64) {
	t.Helper()
	if reason := harness.HeaderValue(m, "x-dlq-reason"); !strings.HasPrefix(reason, reasonPrefix) {
		t.Errorf("expected x-dlq-reason to start with %q, got %q", reasonPrefix, reason)
	}
	if got := harness.HeaderValue(m, "x-dlq-source-topic"); got != wantSourceTopic {
		t.Errorf("expected x-dlq-source-topic=%q, got %q", wantSourceTopic, got)
	}
	partition, perr := strconv.Atoi(harness.HeaderValue(m, "x-dlq-source-partition"))
	if perr != nil {
		t.Errorf("bad x-dlq-source-partition header %q: %v", harness.HeaderValue(m, "x-dlq-source-partition"), perr)
	}
	offset, oerr := strconv.ParseInt(harness.HeaderValue(m, "x-dlq-source-offset"), 10, 64)
	if oerr != nil {
		t.Errorf("bad x-dlq-source-offset header %q: %v", harness.HeaderValue(m, "x-dlq-source-offset"), oerr)
	}
	return partition, offset
}

// TestSeatReservationPoisonMessage_BookingEvents produces a poison message on
// booking.events (event_type=BookingCancelled, so both event-service's
// ReleaseSeat and analytics-service's RecordBookingOutcome attempt it, per
// §3's one-topic-several-event-types rule) and proves the partition is not
// wedged: a well-formed BookingCancelled for a booking-service never created
// still gets a genuine successful application from both groups right after
// (ReleaseSeat's missing-seat_reservations-row case is a documented no-op
// success, §5.1; RecordBookingOutcome inserts a fresh read-model row).
func TestSeatReservationPoisonMessage_BookingEvents(t *testing.T) {
	requireKafka(t)
	ctx := context.Background()

	topic := harness.BookingEventsTopic()
	dlqTopic := harness.DLQTopic(topic)
	syntheticBookingID := uuid.New()
	poisonKey := "poison-" + syntheticBookingID.String()

	if err := harness.WriteToPartition(ctx, topic, 0, kafka.Message{
		Key:     []byte(poisonKey),
		Headers: []kafka.Header{{Key: "event_type", Value: []byte("BookingCancelled")}},
		Value:   []byte("{this is not valid json"),
	}); err != nil {
		t.Fatalf("produce poison message: %v", err)
	}

	dlq, err := harness.FindMessage(ctx, dlqTopic, 20*time.Second, harness.MatchKey(poisonKey))
	if err != nil {
		t.Fatalf("poison message never reached %s (partition may be wedged): %v", dlqTopic, err)
	}
	assertDLQHeaders(t, dlq, topic, "parse:")

	// Watch from here, before producing the follow-up, so we can assert it
	// does NOT also dead-letter.
	watch := harness.NewDLQWatch(ctx, dlqTopic)

	legitEventID := uuid.New()
	legit := bookingCancelledWire{
		EventID:         legitEventID.String(),
		BookingID:       syntheticBookingID.String(),
		UserID:          uuid.NewString(),
		TicketedEventID: uuid.NewString(),
		SeatIDs:         []string{uuid.NewString()},
		Reason:          "reservation_timeout",
		OccurredAt:      time.Now().UTC().Format(time.RFC3339),
	}
	legitBytes, err := json.Marshal(legit)
	if err != nil {
		t.Fatalf("marshal follow-up BookingCancelled: %v", err)
	}
	if err := harness.WriteToPartition(ctx, topic, 0, kafka.Message{
		Key: []byte(syntheticBookingID.String()),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("BookingCancelled")},
			{Key: "event_id", Value: []byte(legitEventID.String())},
		},
		Value: legitBytes,
	}); err != nil {
		t.Fatalf("produce follow-up BookingCancelled: %v", err)
	}

	harness.PollUntil(t, 20*time.Second, 300*time.Millisecond,
		"follow-up BookingCancelled to be recorded by analytics-service (proves the partition kept moving)",
		func() bool {
			var count int
			err := harnessPools.Analytics.QueryRow(ctx, `select count(*) from booking_outcomes where booking_id=$1`, syntheticBookingID).Scan(&count)
			return err == nil && count == 1
		})
	assertBookingOutcome(t, ctx, syntheticBookingID, "cancelled")
	// event-service's ReleaseSeat treats "no seat_reservations row" as a
	// legitimate no-op (§5.1) but still records the event as processed. It is
	// an independent consumer group from analytics-service's, polled above,
	// so it gets its own bounded wait rather than an instant check.
	pollProcessedEventCount(t, ctx, harnessPools.Event, legitEventID, 1)

	watch.AssertEmpty(t, ctx, 3*time.Second)
}

// TestSeatReservationPoisonMessage_SeatReservationEvents produces a poison
// message on seat_reservation.events (event_type=SeatReserved, owned solely
// by booking-service-SeatReserved -- the first Rust/rdkafka consumer in this
// repo) and proves forward progress with a follow-up that reaches its own,
// distinctly-reasoned terminal DLQ outcome rather than repeating (or hanging
// on) the poison one.
//
// Unlike booking.events' BookingCancelled, neither SeatReserved nor
// SeatReservationFailed has a "no matching row" no-op carve-out --
// ConfirmBooking/CancelBooking both require a real `bookings` row
// (docs/sagas/seat-reservation.md §2 steps 3a/3b), and the only public way to
// create one is the full CreateBooking flow. Reaching around that with direct
// SQL to seed a fixture would be exactly the "back door" this tier's ground
// rules rule out, so the follow-up here is a well-formed message for a
// booking-service has never heard of, which is a permanent NotFound
// rejection -- itself proof the consumer moved past the poisoned offset,
// since it is a *different* terminal outcome at a *later* source offset on
// the same partition, not a repeat of the poison message's parse failure.
func TestSeatReservationPoisonMessage_SeatReservationEvents(t *testing.T) {
	requireKafka(t)
	ctx := context.Background()

	topic := harness.SeatReservationEventsTopic()
	dlqTopic := harness.DLQTopic(topic)
	syntheticBookingID := uuid.New()
	poisonKey := "poison-" + syntheticBookingID.String()

	if err := harness.WriteToPartition(ctx, topic, 0, kafka.Message{
		Key:     []byte(poisonKey),
		Headers: []kafka.Header{{Key: "event_type", Value: []byte("SeatReserved")}},
		Value:   []byte("{this is not valid json"),
	}); err != nil {
		t.Fatalf("produce poison message: %v", err)
	}

	dlq, err := harness.FindMessage(ctx, dlqTopic, 20*time.Second, harness.MatchKey(poisonKey))
	if err != nil {
		t.Fatalf("poison message never reached %s (partition may be wedged): %v", dlqTopic, err)
	}
	poisonPartition, poisonOffset := assertDLQHeaders(t, dlq, topic, "parse:")

	legitEventID := uuid.New()
	legit := struct {
		EventID         string   `json:"event_id"`
		BookingID       string   `json:"booking_id"`
		TicketedEventID string   `json:"ticketed_event_id"`
		SeatIDs         []string `json:"seat_ids"`
		ReservedAt      string   `json:"reserved_at"`
	}{
		EventID:         legitEventID.String(),
		BookingID:       syntheticBookingID.String(),
		TicketedEventID: uuid.NewString(),
		SeatIDs:         []string{uuid.NewString()},
		ReservedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	legitBytes, err := json.Marshal(legit)
	if err != nil {
		t.Fatalf("marshal follow-up SeatReserved: %v", err)
	}
	if err := harness.WriteToPartition(ctx, topic, 0, kafka.Message{
		Key: []byte(syntheticBookingID.String()),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("SeatReserved")},
			{Key: "event_id", Value: []byte(legitEventID.String())},
		},
		Value: legitBytes,
	}); err != nil {
		t.Fatalf("produce follow-up SeatReserved: %v", err)
	}

	followUp, err := harness.FindMessage(ctx, dlqTopic, 20*time.Second, harness.MatchKey(syntheticBookingID.String()))
	if err != nil {
		t.Fatalf("follow-up SeatReserved never reached a terminal outcome on %s (partition may be wedged): %v", dlqTopic, err)
	}
	followUpPartition, followUpOffset := assertDLQHeaders(t, followUp, topic, "permanent:")

	if followUpPartition != poisonPartition {
		t.Fatalf("expected the follow-up to originate from the same partition (%d) as the poison message, got %d",
			poisonPartition, followUpPartition)
	}
	if followUpOffset <= poisonOffset {
		t.Fatalf("expected the follow-up's source offset (%d) to be greater than the poison message's (%d) -- partition may be wedged",
			followUpOffset, poisonOffset)
	}
	// processed_events must NOT have a row for this one -- it never
	// succeeded, so it must not be recorded as applied.
	assertProcessedEventCount(t, ctx, harnessPools.Booking, legitEventID, 0)
}
