package usecase

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type ListEventsUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewListEventsUseCase(pool *pgxpool.Pool, repo port.Repository) *ListEventsUseCase {
	return &ListEventsUseCase{pool: pool, repo: repo}
}

func (uc *ListEventsUseCase) Execute(ctx context.Context, f domain.EventFilter, p domain.Pagination) ([]domain.Event, int, error) {
	tx, err := uc.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	events, total, err := uc.repo.ListEvents(ctx, tx, f, p)
	if err != nil {
		return nil, 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	return events, total, nil
}
