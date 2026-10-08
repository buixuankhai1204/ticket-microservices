package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type GetEventUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewGetEventUseCase(db port.Transactor, repo port.Repository) *GetEventUseCase {
	return &GetEventUseCase{db: db, repo: repo}
}

func (uc *GetEventUseCase) Execute(ctx context.Context, id uuid.UUID) (domain.Event, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
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
