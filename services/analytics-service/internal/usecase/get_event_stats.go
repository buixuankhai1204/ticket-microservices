package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
)

type GetEventStatsUseCase struct {
	db   port.Transactor
	repo port.Repository
}

func NewGetEventStatsUseCase(db port.Transactor, repo port.Repository) *GetEventStatsUseCase {
	return &GetEventStatsUseCase{db: db, repo: repo}
}

func (uc *GetEventStatsUseCase) Execute(ctx context.Context, eventID uuid.UUID) (domain.EventBookingStats, error) {
	tx, err := uc.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.EventBookingStats{}, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	stats, err := uc.repo.GetEventBookingStats(ctx, tx, eventID)
	if err != nil {
		return domain.EventBookingStats{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.EventBookingStats{}, &domain.RepositoryError{Err: err}
	}
	return stats, nil
}
