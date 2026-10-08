package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport/mocks"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

func TestEventStatsAreReadInsideAReadOnlyTransaction(t *testing.T) {
	eventID := uuid.New()
	repo := mocks.NewMockRepository(gomock.NewController(t))
	db := &testsupport.FakeDB{}
	want := domain.EventBookingStats{EventID: eventID, Confirmed: 3, Cancelled: 1}
	repo.EXPECT().GetEventBookingStats(gomock.Any(), gomock.Any(), eventID).Return(want, nil)

	got, err := usecase.NewGetEventStatsUseCase(db, repo).Execute(context.Background(), eventID)

	if err != nil || got != want {
		t.Fatalf("got (%+v, %v), want (%+v, nil)", got, err, want)
	}
	if !db.ReadOnly() {
		t.Fatalf("a query must run in a read-only transaction")
	}
}

func TestAMissingUserRegistrationIsReportedAsNotFound(t *testing.T) {
	userID := uuid.New()
	repo := mocks.NewMockRepository(gomock.NewController(t))
	db := &testsupport.FakeDB{}
	repo.EXPECT().GetUserRegistration(gomock.Any(), gomock.Any(), userID).Return(domain.UserRegistration{}, domain.ErrNotFound)

	_, err := usecase.NewGetUserRegistrationUseCase(db, repo).Execute(context.Background(), userID)

	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if db.Committed() {
		t.Fatalf("calls = %v, a failed read must roll back, not commit", db.Calls())
	}
}

func TestQueriesFailWithARepositoryErrorWhenNoConnectionIsAvailable(t *testing.T) {
	repo := mocks.NewMockRepository(gomock.NewController(t))
	db := &testsupport.FakeDB{BeginErr: errors.New("pool exhausted")}

	_, statsErr := usecase.NewGetEventStatsUseCase(db, repo).Execute(context.Background(), uuid.New())
	_, regErr := usecase.NewGetUserRegistrationUseCase(db, repo).Execute(context.Background(), uuid.New())

	for _, err := range []error{statsErr, regErr} {
		var repoErr *domain.RepositoryError
		if !errors.As(err, &repoErr) {
			t.Fatalf("err = %v, want a RepositoryError so the handler answers 500", err)
		}
	}
}
