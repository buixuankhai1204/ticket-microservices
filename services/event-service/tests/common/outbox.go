package common

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const outboxTapSQL = `
CREATE TABLE IF NOT EXISTS outbox_tap (
	seq            BIGSERIAL PRIMARY KEY,
	id             UUID,
	aggregate_id   UUID,
	aggregate_type TEXT,
	event_type     TEXT,
	payload        JSONB
);

CREATE OR REPLACE FUNCTION outbox_tap_copy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO outbox_tap (id, aggregate_id, aggregate_type, event_type, payload)
	VALUES (NEW.id, NEW.aggregate_id, NEW.aggregate_type, NEW.event_type, NEW.payload);
	RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS outbox_tap_copy ON outbox_events;
CREATE TRIGGER outbox_tap_copy AFTER INSERT ON outbox_events
	FOR EACH ROW EXECUTE FUNCTION outbox_tap_copy();
`

type Published struct {
	ID            uuid.UUID
	AggregateID   uuid.UUID
	AggregateType string
	EventType     string
	Payload       map[string]any
}

type OutboxTap struct {
	t    testing.TB
	pool *pgxpool.Pool
}

func TapOutbox(t testing.TB, pool *pgxpool.Pool) *OutboxTap {
	t.Helper()
	if _, err := pool.Exec(context.Background(), outboxTapSQL); err != nil {
		t.Fatalf("install the outbox tap: %v", err)
	}
	return &OutboxTap{t: t, pool: pool}
}

func (o *OutboxTap) Events() []Published {
	o.t.Helper()
	rows, err := o.pool.Query(context.Background(),
		`SELECT id, aggregate_id, aggregate_type, event_type, payload FROM outbox_tap ORDER BY seq`)
	if err != nil {
		o.t.Fatalf("read the outbox tap: %v", err)
	}
	defer rows.Close()

	var out []Published
	for rows.Next() {
		var p Published
		var raw []byte
		if err := rows.Scan(&p.ID, &p.AggregateID, &p.AggregateType, &p.EventType, &raw); err != nil {
			o.t.Fatalf("scan the outbox tap: %v", err)
		}
		if err := json.Unmarshal(raw, &p.Payload); err != nil {
			o.t.Fatalf("decode outbox payload %s: %v", raw, err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		o.t.Fatalf("iterate the outbox tap: %v", err)
	}
	return out
}

func (o *OutboxTap) Types() []string {
	o.t.Helper()
	var types []string
	for _, e := range o.Events() {
		types = append(types, e.EventType)
	}
	return types
}
