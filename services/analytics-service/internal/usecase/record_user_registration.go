package usecase

import (
	"context"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

// RecordUserRegistrationUseCase projects a UserCreated event into the
// user_registrations read model. It is triggered by the Kafka consumer, not by
// an HTTP request. One flow, one type; depends only on the domain.Repository
// port injected via the constructor.
type RecordUserRegistrationUseCase struct {
	repo domain.Repository
}

func NewRecordUserRegistrationUseCase(repo domain.Repository) *RecordUserRegistrationUseCase {
	return &RecordUserRegistrationUseCase{repo: repo}
}

// Execute records the registration. alreadyProcessed is true when this event id
// was applied by an earlier delivery — a successful no-op the caller should
// treat as "done" (commit the offset), not an error. A malformed event yields
// domain.ErrInvalidUserRegistration (permanent); an infrastructure failure
// yields *domain.RepositoryError (retryable). The idempotency check and the
// read-model write share one transaction inside the repository.
func (uc *RecordUserRegistrationUseCase) Execute(ctx context.Context, ev domain.UserCreated) (alreadyProcessed bool, err error) {
	reg, err := domain.NewUserRegistration(ev.UserID, ev.Email, ev.CreatedAt)
	if err != nil {
		return false, err
	}
	return uc.repo.RecordUserRegistration(ctx, ev.EventID, *reg)
}
