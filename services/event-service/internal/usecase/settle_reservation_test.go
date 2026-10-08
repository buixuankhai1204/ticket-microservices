package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

type settlement struct {
	name        string
	seatsAfter  string
	execute     func(db *FakeDB, repo *MockRepository, bookingID uuid.UUID, eventID uuid.UUID) (bool, error)
	finalStatus string
}

func settlements() []settlement {
	return []settlement{
		{
			name:        "confirmation",
			seatsAfter:  domain.SeatBooked,
			finalStatus: domain.ReservationFinalized,
			execute: func(db *FakeDB, repo *MockRepository, bookingID, eventID uuid.UUID) (bool, error) {
				return usecase.NewFinalizeSeatUseCase(db, repo).Execute(context.Background(),
					domain.BookingConfirmed{ID: eventID, BookingID: bookingID})
			},
		},
		{
			name:        "cancellation",
			seatsAfter:  domain.SeatAvailable,
			finalStatus: domain.ReservationReleased,
			execute: func(db *FakeDB, repo *MockRepository, bookingID, eventID uuid.UUID) (bool, error) {
				return usecase.NewReleaseSeatUseCase(db, repo).Execute(context.Background(),
					domain.BookingCancelled{ID: eventID, BookingID: bookingID})
			},
		},
	}
}

func TestSettlingAHeldReservationMovesItsSeatsAndTheReservationTogether(t *testing.T) {
	for _, s := range settlements() {
		t.Run(s.name, func(t *testing.T) {
			bookingID, eventID, seatIDs := uuid.New(), uuid.New(), []uuid.UUID{uuid.New(), uuid.New()}
			repo := NewMockRepository(gomock.NewController(t))
			db := &FakeDB{}
			held := domain.SeatReservation{BookingID: bookingID, SeatIDs: seatIDs, Status: domain.ReservationHeld}
			gomock.InOrder(
				repo.EXPECT().MarkEventProcessed(gomock.Any(), inTx(db), eventID).Return(false, nil),
				repo.EXPECT().LockSeatReservation(gomock.Any(), inTx(db), bookingID).Return(held, nil),
				repo.EXPECT().UpdateSeatsStatus(gomock.Any(), inTx(db), seatIDs, s.seatsAfter).Return(nil),
				repo.EXPECT().UpdateSeatReservationStatus(gomock.Any(), inTx(db), bookingID, s.finalStatus).Return(nil),
			)

			already, err := s.execute(db, repo, bookingID, eventID)

			if err != nil || already || lastCall(db) != "commit" {
				t.Fatalf("got (%v, %v) with calls %v, want (false, nil) ending in commit", already, err, db.Calls())
			}
		})
	}
}

func TestSettlingAReservationThatIsAlreadyDoneChangesNothing(t *testing.T) {
	for _, s := range settlements() {
		t.Run(s.name, func(t *testing.T) {
			bookingID, eventID := uuid.New(), uuid.New()
			repo := NewMockRepository(gomock.NewController(t))
			db := &FakeDB{}
			done := domain.SeatReservation{BookingID: bookingID, SeatIDs: []uuid.UUID{uuid.New()}, Status: s.finalStatus}
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), eventID).Return(false, nil)
			repo.EXPECT().LockSeatReservation(gomock.Any(), gomock.Any(), bookingID).Return(done, nil)
			repo.EXPECT().UpdateSeatsStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			repo.EXPECT().UpdateSeatReservationStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			already, err := s.execute(db, repo, bookingID, eventID)

			if err != nil || already || !db.Committed() {
				t.Fatalf("got (%v, %v) with calls %v, want a committed no-op", already, err, db.Calls())
			}
		})
	}
}

func TestAnOutcomeThatContradictsTheReservationIsRejectedAndNothingIsCommitted(t *testing.T) {
	tests := []struct {
		name    string
		current string
		execute func(db *FakeDB, repo *MockRepository, bookingID, eventID uuid.UUID) (bool, error)
	}{
		{"confirming a released reservation", domain.ReservationReleased, settlements()[0].execute},
		{"cancelling a finalized reservation", domain.ReservationFinalized, settlements()[1].execute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bookingID, eventID := uuid.New(), uuid.New()
			repo := NewMockRepository(gomock.NewController(t))
			db := &FakeDB{}
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), eventID).Return(false, nil)
			repo.EXPECT().LockSeatReservation(gomock.Any(), gomock.Any(), bookingID).
				Return(domain.SeatReservation{BookingID: bookingID, Status: tc.current}, nil)
			repo.EXPECT().UpdateSeatsStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			_, err := tc.execute(db, repo, bookingID, eventID)

			if !errors.Is(err, domain.ErrReservationNotHeld) || db.Committed() {
				t.Fatalf("got err %v with calls %v, want ErrReservationNotHeld and no commit: the processed marker must roll back too", err, db.Calls())
			}
		})
	}
}

func TestAnEventSeenBeforeIsAcknowledgedWithoutLockingAnything(t *testing.T) {
	for _, s := range settlements() {
		t.Run(s.name, func(t *testing.T) {
			bookingID, eventID := uuid.New(), uuid.New()
			repo := NewMockRepository(gomock.NewController(t))
			db := &FakeDB{}
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), eventID).Return(true, nil)

			already, err := s.execute(db, repo, bookingID, eventID)

			if err != nil || !already || !db.Committed() {
				t.Fatalf("got (%v, %v) with calls %v, want (true, nil) committed", already, err, db.Calls())
			}
		})
	}
}

func TestNoReservationRowMeansCancellationIsANoOpButConfirmationIsAnError(t *testing.T) {
	bookingID, eventID := uuid.New(), uuid.New()
	missing := func(repo *MockRepository) {
		repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), eventID).Return(false, nil)
		repo.EXPECT().LockSeatReservation(gomock.Any(), gomock.Any(), bookingID).Return(domain.SeatReservation{}, domain.ErrNotFound)
	}

	t.Run("cancellation of a booking that never held seats", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		db := &FakeDB{}
		missing(repo)

		already, err := settlements()[1].execute(db, repo, bookingID, eventID)

		if err != nil || already || !db.Committed() {
			t.Fatalf("got (%v, %v) with calls %v, want a committed no-op", already, err, db.Calls())
		}
	})
	t.Run("confirmation of a booking nobody reserved", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		db := &FakeDB{}
		missing(repo)

		_, err := settlements()[0].execute(db, repo, bookingID, eventID)

		var repoErr *domain.RepositoryError
		if !errors.As(err, &repoErr) || db.Committed() {
			t.Fatalf("got err %v with calls %v, want a retryable RepositoryError: the reservation may simply not have landed yet", err, db.Calls())
		}
	})
}
