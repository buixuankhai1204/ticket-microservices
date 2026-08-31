package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type GetEventUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewGetEventUseCase(pool *pgxpool.Pool, repo port.Repository) *GetEventUseCase {
	return &GetEventUseCase{pool: pool, repo: repo}
}

func (uc *GetEventUseCase) Execute(ctx context.Context, id uuid.UUID) (domain.Event, error) {
	tx, err := uc.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Event{}, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	event, err := uc.repo.GetEvent(ctx, tx, id)
	if err != nil {
		return domain.Event{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Event{}, &domain.RepositoryError{Err: err}
	}
	return event, nil
}
