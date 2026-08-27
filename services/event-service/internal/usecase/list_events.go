package usecase

import (
	"context"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

// ListEventsUseCase serves the event-browsing list. One type per business flow;
// it depends only on the domain.Repository port, injected via the constructor.
type ListEventsUseCase struct {
	repo domain.Repository
}

func NewListEventsUseCase(repo domain.Repository) *ListEventsUseCase {
	return &ListEventsUseCase{repo: repo}
}

// Execute returns one page of events matching the filter (newest first) and the
// total match count for the response envelope. It parses nothing itself — the
// HTTP layer hands it an already-validated domain.Pagination. The single
// read-only transaction that spans the page + count lives in the repository
// implementation (CLAUDE.md: every endpoint's DB access runs in one transaction).
func (uc *ListEventsUseCase) Execute(ctx context.Context, f domain.EventFilter, p domain.Pagination) ([]domain.Event, int, error) {
	return uc.repo.ListEvents(ctx, f, p)
}
