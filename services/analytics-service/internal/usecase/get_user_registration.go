package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type GetUserRegistrationUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewGetUserRegistrationUseCase(db port.Transactor, repo port.Repository) *GetUserRegistrationUseCase {
	return &GetUserRegistrationUseCase{db: db, repo: repo}
}

func (uc *GetUserRegistrationUseCase) Execute(ctx context.Context, userID uuid.UUID) (domain.UserRegistration, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
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
