package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

// GetEventStatsUseCase returns aggregate booking analytics for one event. One
// type per business flow; it depends only on the domain.Repository port, which
// is injected via the constructor.
type GetEventStatsUseCase struct {
	repo domain.Repository
}

func NewGetEventStatsUseCase(repo domain.Repository) *GetEventStatsUseCase {
	return &GetEventStatsUseCase{repo: repo}
}

// Execute fetches the confirmed / cancelled counts for the given event. The
// single read-only transaction that spans the read lives in the repository
// implementation (CLAUDE.md: every endpoint's DB access runs in one transaction).
func (uc *GetEventStatsUseCase) Execute(ctx context.Context, eventID uuid.UUID) (domain.EventBookingStats, error) {
	return uc.repo.GetEventBookingStats(ctx, eventID)
}
