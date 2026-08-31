package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type GetUserRegistrationUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewGetUserRegistrationUseCase(pool *pgxpool.Pool, repo port.Repository) *GetUserRegistrationUseCase {
	return &GetUserRegistrationUseCase{pool: pool, repo: repo}
}

func (uc *GetUserRegistrationUseCase) Execute(ctx context.Context, userID uuid.UUID) (domain.UserRegistration, error) {
	tx, err := uc.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.UserRegistration{}, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	reg, err := uc.repo.GetUserRegistration(ctx, tx, userID)
	if err != nil {
		return domain.UserRegistration{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.UserRegistration{}, &domain.RepositoryError{Err: err}
	}
	return reg, nil
}
