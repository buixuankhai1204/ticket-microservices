package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

// ListEventSeatsUseCase serves the seat map for one event. One flow; depends
// only on the domain.Repository port.
type ListEventSeatsUseCase struct {
	repo domain.Repository
}

func NewListEventSeatsUseCase(repo domain.Repository) *ListEventSeatsUseCase {
	return &ListEventSeatsUseCase{repo: repo}
}

// Execute returns one page of the event's seats plus the total seat count, or
// domain.ErrNotFound if the event does not exist. The single read-only
// transaction lives in the repository implementation.
func (uc *ListEventSeatsUseCase) Execute(ctx context.Context, eventID uuid.UUID, p domain.Pagination) ([]domain.Seat, int, error) {
	return uc.repo.ListSeatsForEvent(ctx, eventID, p)
}
