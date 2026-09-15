//go:build e2e

package harness

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pools holds one *pgxpool.Pool per participant database in the
// seat-reservation saga (and user-service's, for any future saga that needs
// it). Tests query these directly for assertions -- only saga *actions* go
// through Kong.
type Pools struct {
	Booking   *pgxpool.Pool
	Event     *pgxpool.Pool
	Analytics *pgxpool.Pool
	User      *pgxpool.Pool
}

func OpenPools(ctx context.Context) (*Pools, error) {
	booking, err := pgxpool.New(ctx, BookingDatabaseURL())
	if err != nil {
		return nil, fmt.Errorf("open booking db pool: %w", err)
	}
	event, err := pgxpool.New(ctx, EventDatabaseURL())
	if err != nil {
		booking.Close()
		return nil, fmt.Errorf("open event db pool: %w", err)
	}
	analytics, err := pgxpool.New(ctx, AnalyticsDatabaseURL())
	if err != nil {
		booking.Close()
		event.Close()
		return nil, fmt.Errorf("open analytics db pool: %w", err)
	}
	user, err := pgxpool.New(ctx, UserDatabaseURL())
	if err != nil {
		booking.Close()
		event.Close()
		analytics.Close()
		return nil, fmt.Errorf("open user db pool: %w", err)
	}
	return &Pools{Booking: booking, Event: event, Analytics: analytics, User: user}, nil
}

// Ping fails fast if any participant DB is unreachable, so TestMain can skip
// cleanly instead of every test timing out individually on its first query.
func (p *Pools) Ping(ctx context.Context) error {
	named := []struct {
		name string
		pool *pgxpool.Pool
	}{
		{"booking", p.Booking},
		{"event", p.Event},
		{"analytics", p.Analytics},
		{"user", p.User},
	}
	for _, n := range named {
		if err := n.pool.Ping(ctx); err != nil {
			return fmt.Errorf("%s db unreachable: %w", n.name, err)
		}
	}
	return nil
}

func (p *Pools) Close() {
	p.Booking.Close()
	p.Event.Close()
	p.Analytics.Close()
	p.User.Close()
}
