package kafka_test

import (
	"context"
	"errors"
	"testing"

	segkafka "github.com/segmentio/kafka-go"

	kafka "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
)

type fakeHandler struct {
	outcome kafka.Outcome
	err     error
	calls   int
}

func (h *fakeHandler) Process(context.Context, segkafka.Message) (kafka.Outcome, error) {
	h.calls++
	return h.outcome, h.err
}

func TestAMessageGoesToTheHandlerOfItsTopicAndStopsAtTheFirstOneThatOwnsIt(t *testing.T) {
	skips := &fakeHandler{outcome: kafka.Ignored}
	owner := &fakeHandler{outcome: kafka.Handled}
	later := &fakeHandler{outcome: kafka.Handled}
	other := &fakeHandler{outcome: kafka.Handled}
	inbox := kafka.Inbox{
		{Topic: "user.events", Handler: skips},
		{Topic: "user.events", Handler: owner},
		{Topic: "user.events", Handler: later},
		{Topic: "booking.events", Handler: other},
	}

	outcome, err := inbox.Deliver(context.Background(), segkafka.Message{Topic: "user.events"})

	if err != nil || outcome != kafka.Handled {
		t.Fatalf("got (%v, %v), want (handled, nil)", outcome, err)
	}
	if skips.calls != 1 || owner.calls != 1 || later.calls != 0 || other.calls != 0 {
		t.Fatalf("calls = skips %d owner %d later %d other-topic %d, want 1/1/0/0", skips.calls, owner.calls, later.calls, other.calls)
	}
}

func TestAMessageNobodyOwnsIsIgnoredAndAnErrorIsNeverSwallowed(t *testing.T) {
	boom := errors.New("dlq down")
	inbox := kafka.Inbox{
		{Topic: "user.events", Handler: &fakeHandler{outcome: kafka.Ignored}},
		{Topic: "booking.events", Handler: &fakeHandler{err: boom}},
	}

	outcome, err := inbox.Deliver(context.Background(), segkafka.Message{Topic: "user.events"})
	if err != nil || outcome != kafka.Ignored {
		t.Fatalf("got (%v, %v), want (ignored, nil)", outcome, err)
	}

	if _, err := inbox.Deliver(context.Background(), segkafka.Message{Topic: "booking.events"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
