package port

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Transactor interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}
