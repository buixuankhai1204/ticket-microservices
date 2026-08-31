package usecase

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/port"
)

type CreateNewEventInput struct {
	Name        string
	Description string
	Venue       string
	StartsAt    time.Time
	EndsAt      time.Time
	Layout      domain.LayoutSpec
}

type CreateNewEventUseCase struct {
	pool *pgxpool.Pool
	repo port.Repository
}

func NewCreateNewEventUseCase(pool *pgxpool.Pool, repo port.Repository) *CreateNewEventUseCase {
	return &CreateNewEventUseCase{pool: pool, repo: repo}
}

func (uc *CreateNewEventUseCase) Execute(ctx context.Context, in CreateNewEventInput) (domain.Event, []domain.Seat, error) {
	event, seats, err := domain.NewEventWithSeats(
		in.Name, in.Description, in.Venue, in.StartsAt, in.EndsAt, in.Layout,
	)
	if err != nil {
		return domain.Event{}, nil, err
	}

	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return domain.Event{}, nil, &domain.RepositoryError{Err: err}
	}
	defer tx.Rollback(ctx)

	if err := uc.repo.CreateEventWithSeats(ctx, tx, *event, seats); err != nil {
		return domain.Event{}, nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Event{}, nil, &domain.RepositoryError{Err: err}
	}
	return *event, seats, nil
}
