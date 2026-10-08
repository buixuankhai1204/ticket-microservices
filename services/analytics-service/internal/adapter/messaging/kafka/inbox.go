package kafka

import (
	"context"

	segkafka "github.com/segmentio/kafka-go"
)

type Route struct {
	Topic   string
	Handler Handler
}

type Inbox []Route

func (in Inbox) Deliver(ctx context.Context, m segkafka.Message) (Outcome, error) {
	for _, route := range in {
		if route.Topic != m.Topic {
			continue
		}
		outcome, err := route.Handler.Process(ctx, m)
		if err != nil || outcome != Ignored {
			return outcome, err
		}
	}
	return Ignored, nil
}
