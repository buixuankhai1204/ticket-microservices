package usecase

import (
	"context"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type RecordProductOrderCancelledUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewRecordProductOrderCancelledUseCase(db port.Transactor, repo port.Repository) *RecordProductOrderCancelledUseCase {
	return &RecordProductOrderCancelledUseCase{db: db, repo: repo}
}

func (uc *RecordProductOrderCancelledUseCase) Execute(ctx context.Context, ev domain.ProductOrderCancelled) (alreadyProcessed bool, err error) {
	outcome, err := domain.NewProductOrderCancelledOutcome(ev)
	if err != nil {
		return false, err
	}

	tx, err := uc.db.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	already, err := uc.repo.RecordProductOrderCancelled(ctx, tx, ev.EventID, *outcome)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return already, nil
}
