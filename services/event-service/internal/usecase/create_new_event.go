package usecase

import (
	"context"
	"time"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
)

// CreateNewEventInput is the already-decoded, transport-free input for creating
// an event and its seats. No http.Request / pgx types appear here. Seats are
// described as a layout (rectangular sections + per-seat exceptions), which the
// domain expands.
type CreateNewEventInput struct {
	Name        string
	Description string
	Venue       string
	StartsAt    time.Time
	EndsAt      time.Time
	Layout      domain.LayoutSpec
}

// CreateNewEventUseCase creates an event together with its full seat map. One
// type per business flow; it depends only on the domain.Repository port,
// injected via the constructor.
type CreateNewEventUseCase struct {
	repo domain.Repository
}

func NewCreateNewEventUseCase(repo domain.Repository) *CreateNewEventUseCase {
	return &CreateNewEventUseCase{repo: repo}
}

// Execute builds the event and its seats — every invariant is enforced in
// domain.NewEventWithSeats, not here — then persists them in the single
// read-write transaction owned by the repository implementation. It returns the
// created event and its seats.
func (uc *CreateNewEventUseCase) Execute(ctx context.Context, in CreateNewEventInput) (domain.Event, []domain.Seat, error) {
	event, seats, err := domain.NewEventWithSeats(
		in.Name, in.Description, in.Venue, in.StartsAt, in.EndsAt, in.Layout,
	)
	if err != nil {
		return domain.Event{}, nil, err
	}

	if err := uc.repo.CreateEventWithSeats(ctx, *event, seats); err != nil {
		return domain.Event{}, nil, err
	}
	return *event, seats, nil
}
