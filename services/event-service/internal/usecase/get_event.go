package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

// GetEventUseCase serves a single event by ID. One flow; depends only on the
// domain.Repository port.
type GetEventUseCase struct {
	repo domain.Repository
}

func NewGetEventUseCase(repo domain.Repository) *GetEventUseCase {
	return &GetEventUseCase{repo: repo}
}

// Execute returns the event, or domain.ErrNotFound. The single read-only
// transaction lives in the repository implementation.
func (uc *GetEventUseCase) Execute(ctx context.Context, id uuid.UUID) (domain.Event, error) {
	return uc.repo.GetEvent(ctx, id)
}
