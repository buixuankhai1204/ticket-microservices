package usecase

import (
	"context"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type RecordProductOrderPlacedUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewRecordProductOrderPlacedUseCase(db port.Transactor, repo port.Repository) *RecordProductOrderPlacedUseCase {
	return &RecordProductOrderPlacedUseCase{db: db, repo: repo}
}

func (uc *RecordProductOrderPlacedUseCase) Execute(ctx context.Context, ev domain.ProductOrderPlaced) (alreadyProcessed bool, err error) {
	outcome, err := domain.NewProductOrderPlacedOutcome(ev)
	if err != nil {
		return false, err
	}

	tx, err := uc.db.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	already, err := uc.repo.RecordProductOrderPlaced(ctx, tx, ev.EventID, *outcome)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return already, nil
}
