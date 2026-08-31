package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type ListEventSeatsUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewListEventSeatsUseCase(pool *pgxpool.Pool, repo port.Repository) *ListEventSeatsUseCase {
	return &ListEventSeatsUseCase{pool: pool, repo: repo}
}

func (uc *ListEventSeatsUseCase) Execute(ctx context.Context, eventID uuid.UUID, p domain.Pagination) ([]domain.Seat, int, error) {
	tx, err := uc.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	seats, total, err := uc.repo.ListSeatsForEvent(ctx, tx, eventID, p)
	if err != nil {
		return nil, 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, &domain.RepositoryError{Err: err}
	}
	return seats, total, nil
}
