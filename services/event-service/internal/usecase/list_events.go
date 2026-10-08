package usecase

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type ListEventsUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewListEventsUseCase(db port.Transactor, repo port.Repository) *ListEventsUseCase {
	return &ListEventsUseCase{db: db, repo: repo}
}

func (uc *ListEventsUseCase) Execute(ctx context.Context, f domain.EventFilter, p domain.Pagination) ([]domain.Event, int, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
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
