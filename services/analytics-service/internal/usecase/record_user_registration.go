package usecase

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type RecordUserRegistrationUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewRecordUserRegistrationUseCase(pool *pgxpool.Pool, repo port.Repository) *RecordUserRegistrationUseCase {
	return &RecordUserRegistrationUseCase{pool: pool, repo: repo}
}

func (uc *RecordUserRegistrationUseCase) Execute(ctx context.Context, ev domain.UserCreated) (alreadyProcessed bool, err error) {
	reg, err := domain.NewUserRegistration(ev.UserID, ev.Email, ev.CreatedAt)
	if err != nil {
		return false, err
	}

	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	already, err := uc.repo.RecordUserRegistration(ctx, tx, ev.EventID, *reg)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return already, nil
}
