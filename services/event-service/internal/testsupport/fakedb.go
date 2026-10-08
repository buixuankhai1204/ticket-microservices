package testsupport

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

var _ port.Transactor = (*FakeDB)(nil)

type FakeDB struct {
	BeginErr  error
	CommitErr error

	mu       sync.Mutex
	calls    []string
	tx       *FakeTx
	readOnly bool
}

type FakeTx struct {
	pgx.Tx
	db     *FakeDB
	closed bool
}

func (d *FakeDB) Begin(ctx context.Context) (pgx.Tx, error) {
	return d.begin(pgx.TxOptions{})
}

func (d *FakeDB) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	return d.begin(opts)
}

func (d *FakeDB) begin(opts pgx.TxOptions) (pgx.Tx, error) {
	d.Note("begin")
	if d.BeginErr != nil {
		return nil, d.BeginErr
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tx = &FakeTx{db: d}
	d.readOnly = opts.AccessMode == pgx.ReadOnly
	return d.tx, nil
}

func (d *FakeDB) Note(call string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, call)
}

func (d *FakeDB) Calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func (d *FakeDB) Begun() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.tx != nil
}

func (d *FakeDB) ReadOnly() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.readOnly
}

func (d *FakeDB) IsCurrentTx(tx pgx.Tx) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.tx != nil && pgx.Tx(d.tx) == tx
}

func (d *FakeDB) Committed() bool {
	for _, c := range d.Calls() {
		if c == "commit" {
			return true
		}
	}
	return false
}

func (t *FakeTx) Commit(ctx context.Context) error {
	t.db.Note("commit")
	if t.db.CommitErr != nil {
		return t.db.CommitErr
	}
	t.closed = true
	return nil
}

func (t *FakeTx) Rollback(ctx context.Context) error {
	if !t.closed {
		t.closed = true
		t.db.Note("rollback")
	}
	return nil
}
