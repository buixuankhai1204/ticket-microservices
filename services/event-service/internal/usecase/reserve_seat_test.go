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

type reservation struct {
	request domain.BookingRequested
	seatIDs []uuid.UUID
}

func newReservation(seats int) reservation {
	r := reservation{request: domain.BookingRequested{
		ID: uuid.New(), BookingID: uuid.New(), UserID: uuid.New(), TicketedEventID: uuid.New(),
	}}
	for range seats {
		r.seatIDs = append(r.seatIDs, uuid.New())
	}
	r.request.SeatIDs = r.seatIDs
	return r
}

func (r reservation) seats(statuses ...string) []domain.Seat {
	out := make([]domain.Seat, len(statuses))
	for i, status := range statuses {
		out[i] = domain.Seat{ID: r.seatIDs[i], EventID: r.request.TicketedEventID, Status: status}
	}
	return out
}

func outboxOf(eventType string, bookingID uuid.UUID) gomock.Matcher {
	return gomock.Cond(func(ev domain.OutboxEvent) bool {
		return ev.EventType() == eventType && ev.AggregateID() == bookingID
	})
}

func failureWith(reason string, bookingID uuid.UUID) gomock.Matcher {
	return gomock.Cond(func(ev domain.OutboxEvent) bool {
		failed, ok := ev.(domain.SeatReservationFailedEvent)
		return ok && failed.Reason == reason && failed.BookingID == bookingID
	})
}

func TestFreeSeatsAreHeldAndTheOutcomeIsPublishedInTheSameTransaction(t *testing.T) {
	r := newReservation(2)
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	ctx := gomock.Any()
	gomock.InOrder(
		repo.EXPECT().MarkEventProcessed(ctx, inTx(db), r.request.ID).Return(false, nil),
		repo.EXPECT().LockSeatsForReservation(ctx, inTx(db), r.request.TicketedEventID, r.seatIDs).
			Return(r.seats(domain.SeatAvailable, domain.SeatAvailable), nil),
		repo.EXPECT().UpdateSeatsStatus(ctx, inTx(db), r.seatIDs, domain.SeatReserved).Return(nil),
		repo.EXPECT().CreateSeatReservation(ctx, inTx(db), r.request.BookingID, r.request.TicketedEventID, r.seatIDs).Return(nil),
		repo.EXPECT().WriteOutbox(ctx, inTx(db), outboxOf("SeatReserved", r.request.BookingID)).Return(nil),
	)

	already, err := usecase.NewReserveSeatUseCase(db, repo).Execute(context.Background(), r.request)

	if err != nil || already {
		t.Fatalf("got (%v, %v), want (false, nil)", already, err)
	}
	if lastCall(db) != "commit" {
		t.Fatalf("calls = %v, want the commit to come last", db.Calls())
	}
}

func TestAReservationThatCannotBeHonouredIsAnsweredWithAFailureAndLeavesEverySeatAlone(t *testing.T) {
	tests := []struct {
		name       string
		lock       func(r reservation) ([]domain.Seat, error)
		wantReason string
	}{
		{"one of the seats is already reserved", func(r reservation) ([]domain.Seat, error) {
			return r.seats(domain.SeatAvailable, domain.SeatReserved), nil
		}, domain.ReasonSeatUnavailable},
		{"one of the seats is already booked", func(r reservation) ([]domain.Seat, error) {
			return r.seats(domain.SeatBooked, domain.SeatAvailable), nil
		}, domain.ReasonSeatUnavailable},
		{"one of the seats does not belong to the event", func(r reservation) ([]domain.Seat, error) {
			return r.seats(domain.SeatAvailable), nil
		}, domain.ReasonSeatNotFound},
		{"the event does not exist", func(r reservation) ([]domain.Seat, error) {
			return nil, domain.ErrNotFound
		}, domain.ReasonEventNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newReservation(2)
			repo := NewMockRepository(gomock.NewController(t))
			db := &FakeDB{}
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), r.request.ID).Return(false, nil)
			seats, lockErr := tc.lock(r)
			repo.EXPECT().LockSeatsForReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(seats, lockErr)
			repo.EXPECT().UpdateSeatsStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			repo.EXPECT().CreateSeatReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			repo.EXPECT().WriteOutbox(gomock.Any(), inTx(db), failureWith(tc.wantReason, r.request.BookingID)).Return(nil)

			already, err := usecase.NewReserveSeatUseCase(db, repo).Execute(context.Background(), r.request)

			if err != nil || already {
				t.Fatalf("got (%v, %v): a refused reservation is a normal outcome, not an error", already, err)
			}
			if lastCall(db) != "commit" {
				t.Fatalf("calls = %v, the failure event must be committed", db.Calls())
			}
		})
	}
}

func TestAnEventAlreadyProcessedIsAcknowledgedWithoutTouchingSeatsOrPublishing(t *testing.T) {
	r := newReservation(1)
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), r.request.ID).Return(true, nil)

	already, err := usecase.NewReserveSeatUseCase(db, repo).Execute(context.Background(), r.request)

	if err != nil || !already {
		t.Fatalf("got (%v, %v), want (true, nil)", already, err)
	}
	if !db.Committed() {
		t.Fatalf("calls = %v, want the processed marker committed", db.Calls())
	}
}

func TestAnyFailureWhileHoldingSeatsRollsBackAndPublishesNothing(t *testing.T) {
	boom := &domain.RepositoryError{Err: errors.New("serialization failure")}
	steps := map[string]func(r reservation, repo *MockRepository){
		"marking the event processed": func(r reservation, repo *MockRepository) {
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, boom)
		},
		"locking the seats": func(r reservation, repo *MockRepository) {
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().LockSeatsForReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, boom)
		},
		"updating the seats": func(r reservation, repo *MockRepository) {
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().LockSeatsForReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(r.seats(domain.SeatAvailable), nil)
			repo.EXPECT().UpdateSeatsStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(boom)
		},
		"recording the reservation": func(r reservation, repo *MockRepository) {
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().LockSeatsForReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(r.seats(domain.SeatAvailable), nil)
			repo.EXPECT().UpdateSeatsStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			repo.EXPECT().CreateSeatReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(boom)
		},
		"writing the outbox": func(r reservation, repo *MockRepository) {
			repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().LockSeatsForReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(r.seats(domain.SeatAvailable), nil)
			repo.EXPECT().UpdateSeatsStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			repo.EXPECT().CreateSeatReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			repo.EXPECT().WriteOutbox(gomock.Any(), gomock.Any(), gomock.Any()).Return(boom)
		},
	}
	for name, arrange := range steps {
		t.Run(name, func(t *testing.T) {
			r := newReservation(1)
			repo := NewMockRepository(gomock.NewController(t))
			db := &FakeDB{}
			arrange(r, repo)

			_, err := usecase.NewReserveSeatUseCase(db, repo).Execute(context.Background(), r.request)

			if err != error(boom) {
				t.Fatalf("err = %v, want the repository error untouched so the consumer can classify it", err)
			}
			if db.Committed() {
				t.Fatalf("calls = %v: a half-applied reservation must never be committed", db.Calls())
			}
		})
	}
}

func TestReservingFailsWithARepositoryErrorWhenNoTransactionCanBeOpenedOrCommitted(t *testing.T) {
	r := newReservation(1)

	t.Run("begin", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		_, err := usecase.NewReserveSeatUseCase(&FakeDB{BeginErr: errors.New("pool exhausted")}, repo).
			Execute(context.Background(), r.request)

		var repoErr *domain.RepositoryError
		if !errors.As(err, &repoErr) {
			t.Fatalf("err = %v, want a RepositoryError", err)
		}
	})
	t.Run("commit", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		repo.EXPECT().MarkEventProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().LockSeatsForReservation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, domain.ErrNotFound)
		repo.EXPECT().WriteOutbox(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		_, err := usecase.NewReserveSeatUseCase(&FakeDB{CommitErr: errors.New("connection reset")}, repo).
			Execute(context.Background(), r.request)

		var repoErr *domain.RepositoryError
		if !errors.As(err, &repoErr) {
			t.Fatalf("err = %v, want a RepositoryError", err)
		}
	})
}
