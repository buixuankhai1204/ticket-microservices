package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

func TestTheReaperReleasesEveryStaleHoldWithTwoBatchedStatements(t *testing.T) {
	seatsA, seatsB := []uuid.UUID{uuid.New(), uuid.New()}, []uuid.UUID{uuid.New()}
	stale := []domain.SeatReservation{
		{BookingID: uuid.New(), SeatIDs: seatsA, Status: domain.ReservationHeld},
		{BookingID: uuid.New(), SeatIDs: seatsB, Status: domain.ReservationHeld},
	}
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	allSeats := append(append([]uuid.UUID{}, seatsA...), seatsB...)
	gomock.InOrder(
		repo.EXPECT().ListStaleHeldReservations(gomock.Any(), inTx(db), 1800).Return(stale, nil),
		repo.EXPECT().ReleaseReservedSeats(gomock.Any(), inTx(db), allSeats).Return(nil),
		repo.EXPECT().UpdateSeatReservationStatusBatch(gomock.Any(), inTx(db),
			[]uuid.UUID{stale[0].BookingID, stale[1].BookingID}, domain.ReservationReleased).Return(nil),
	)

	reaped, err := usecase.NewReapHeldReservationsUseCase(db, repo, 1800).Execute(context.Background())

	if err != nil || reaped != 2 || lastCall(db) != "commit" {
		t.Fatalf("got (%d, %v) with calls %v, want (2, nil) ending in commit", reaped, err, db.Calls())
	}
}

func TestTheReaperDoesNothingWhenNothingIsStale(t *testing.T) {
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	repo.EXPECT().ListStaleHeldReservations(gomock.Any(), gomock.Any(), 60).Return(nil, nil)
	repo.EXPECT().ReleaseReservedSeats(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	repo.EXPECT().UpdateSeatReservationStatusBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	reaped, err := usecase.NewReapHeldReservationsUseCase(db, repo, 60).Execute(context.Background())

	if err != nil || reaped != 0 || !db.Committed() {
		t.Fatalf("got (%d, %v) with calls %v, want a committed no-op", reaped, err, db.Calls())
	}
}

func TestTheReaperCommitsNothingWhenReleasingSeatsFails(t *testing.T) {
	boom := &domain.RepositoryError{Err: errors.New("deadlock detected")}
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	repo.EXPECT().ListStaleHeldReservations(gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]domain.SeatReservation{{BookingID: uuid.New(), SeatIDs: []uuid.UUID{uuid.New()}, Status: domain.ReservationHeld}}, nil)
	repo.EXPECT().ReleaseReservedSeats(gomock.Any(), gomock.Any(), gomock.Any()).Return(boom)
	repo.EXPECT().UpdateSeatReservationStatusBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	reaped, err := usecase.NewReapHeldReservationsUseCase(db, repo, 60).Execute(context.Background())

	if err != error(boom) || reaped != 0 || db.Committed() {
		t.Fatalf("got (%d, %v) with calls %v, want (0, the repository error) and no commit", reaped, err, db.Calls())
	}
}

func TestCreatingAnEventStoresItWithItsSeatsInOneTransaction(t *testing.T) {
	start := time.Date(2030, 6, 1, 19, 0, 0, 0, time.UTC)
	in := usecase.CreateNewEventInput{
		Name: "Gala", Venue: "Hall", StartsAt: start, EndsAt: start.Add(time.Hour),
		Layout: domain.LayoutSpec{Sections: []domain.SectionSpec{{Name: "A", Rows: 2, SeatsPerRow: 3, PriceMinor: 100}}},
	}
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	repo.EXPECT().
		CreateEventWithSeats(gomock.Any(), inTx(db), gomock.Cond(func(e domain.Event) bool { return e.Name == "Gala" }),
			gomock.Cond(func(seats []domain.Seat) bool { return len(seats) == 6 })).
		Return(nil)

	event, seats, err := usecase.NewCreateNewEventUseCase(db, repo).Execute(context.Background(), in)

	if err != nil || len(seats) != 6 || event.Name != "Gala" || lastCall(db) != "commit" {
		t.Fatalf("got (%+v, %d seats, %v) with calls %v", event, len(seats), err, db.Calls())
	}
}

func TestAnInvalidEventNeverOpensATransaction(t *testing.T) {
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}

	_, _, err := usecase.NewCreateNewEventUseCase(db, repo).Execute(context.Background(), usecase.CreateNewEventInput{Name: ""})

	if !errors.Is(err, domain.ErrInvalidEvent) || db.Begun() {
		t.Fatalf("got err %v, begun %v, want ErrInvalidEvent without pinning a connection", err, db.Begun())
	}
}

func TestStoringAnEventRollsBackWhenTheRepositoryFails(t *testing.T) {
	start := time.Date(2030, 6, 1, 19, 0, 0, 0, time.UTC)
	boom := &domain.RepositoryError{Err: errors.New("bulk insert failed")}
	repo := NewMockRepository(gomock.NewController(t))
	db := &FakeDB{}
	repo.EXPECT().CreateEventWithSeats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(boom)

	_, _, err := usecase.NewCreateNewEventUseCase(db, repo).Execute(context.Background(), usecase.CreateNewEventInput{
		Name: "Gala", Venue: "Hall", StartsAt: start, EndsAt: start.Add(time.Hour),
		Layout: domain.LayoutSpec{Sections: []domain.SectionSpec{{Name: "A", Rows: 1, SeatsPerRow: 1}}},
	})

	if err != error(boom) || db.Committed() {
		t.Fatalf("got err %v with calls %v, want the repository error and no commit", err, db.Calls())
	}
}

func TestReadsRunInAReadOnlyTransactionAndPassThroughWhatTheRepositoryReturns(t *testing.T) {
	eventID := uuid.New()
	p := domain.Pagination{Limit: 20}

	t.Run("get event", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		db := &FakeDB{}
		repo.EXPECT().GetEvent(gomock.Any(), inTx(db), eventID).Return(domain.Event{ID: eventID, Name: "Gala"}, nil)

		event, err := usecase.NewGetEventUseCase(db, repo).Execute(context.Background(), eventID)

		if err != nil || event.Name != "Gala" || !db.ReadOnly() {
			t.Fatalf("got (%+v, %v), read-only %v", event, err, db.ReadOnly())
		}
	})
	t.Run("list events", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		db := &FakeDB{}
		filter := domain.EventFilter{UpcomingOnly: true}
		repo.EXPECT().ListEvents(gomock.Any(), inTx(db), filter, p).Return([]domain.Event{{ID: eventID}}, 41, nil)

		events, total, err := usecase.NewListEventsUseCase(db, repo).Execute(context.Background(), filter, p)

		if err != nil || len(events) != 1 || total != 41 || !db.ReadOnly() {
			t.Fatalf("got (%d events, total %d, %v), read-only %v", len(events), total, err, db.ReadOnly())
		}
	})
	t.Run("list seats of an unknown event", func(t *testing.T) {
		repo := NewMockRepository(gomock.NewController(t))
		db := &FakeDB{}
		repo.EXPECT().ListSeatsForEvent(gomock.Any(), inTx(db), eventID, p).Return(nil, 0, domain.ErrNotFound)

		_, _, err := usecase.NewListEventSeatsUseCase(db, repo).Execute(context.Background(), eventID, p)

		if !errors.Is(err, domain.ErrNotFound) || !db.ReadOnly() || db.Committed() {
			t.Fatalf("got err %v with calls %v, want ErrNotFound from a read-only transaction that rolled back", err, db.Calls())
		}
	})
}
