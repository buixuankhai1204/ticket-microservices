package usecase

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type RecordUserLoginUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewRecordUserLoginUseCase(pool *pgxpool.Pool, repo port.Repository) *RecordUserLoginUseCase {
	return &RecordUserLoginUseCase{pool: pool, repo: repo}
}

func (uc *RecordUserLoginUseCase) Execute(ctx context.Context, ev domain.UserLoggedIn) (alreadyProcessed bool, err error) {
	login, err := domain.NewUserLogin(ev.UserID, ev.Email, ev.LoggedInAt)
	if err != nil {
		return false, err
	}

	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	already, err := uc.repo.RecordUserLogin(ctx, tx, ev.EventID, *login)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, &domain.RepositoryError{Err: err}
	}
	return already, nil
}
