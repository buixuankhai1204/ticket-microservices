package usecase

import (
	"context"

	"github.com/google/uuid"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
)

// GetUserRegistrationUseCase serves the read side of the user-registration
// projection. One flow; depends only on the domain.Repository port.
type GetUserRegistrationUseCase struct {
	repo domain.Repository
}

func NewGetUserRegistrationUseCase(repo domain.Repository) *GetUserRegistrationUseCase {
	return &GetUserRegistrationUseCase{repo: repo}
}

// Execute returns the projected registration for a user, or domain.ErrNotFound.
// The single read-only transaction lives in the repository implementation.
func (uc *GetUserRegistrationUseCase) Execute(ctx context.Context, userID uuid.UUID) (domain.UserRegistration, error) {
	return uc.repo.GetUserRegistration(ctx, userID)
}
