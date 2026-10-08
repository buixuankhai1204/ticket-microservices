package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/mock/gomock"

	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/domain"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/port"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/testsupport/mocks"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

type recordCase struct {
	name    string
	expect  func(repo *mocks.MockRepository) *gomock.Call
	execute func(db port.Transactor, repo port.Repository) (bool, error)
}

func recordCases() []recordCase {
	now := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	return []recordCase{
		{
			name: "user registration",
			expect: func(repo *mocks.MockRepository) *gomock.Call {
				return repo.EXPECT().RecordUserRegistration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			},
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordUserRegistrationUseCase(db, repo).Execute(context.Background(), domain.UserCreated{
					EventID: uuid.New(), UserID: uuid.New(), Email: "ada@example.com", CreatedAt: now,
				})
			},
		},
		{
			name: "user login",
			expect: func(repo *mocks.MockRepository) *gomock.Call {
				return repo.EXPECT().RecordUserLogin(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			},
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordUserLoginUseCase(db, repo).Execute(context.Background(), domain.UserLoggedIn{
					EventID: uuid.New(), UserID: uuid.New(), Email: "ada@example.com", LoggedInAt: now,
				})
			},
		},
		{
			name: "booking confirmed",
			expect: func(repo *mocks.MockRepository) *gomock.Call {
				return repo.EXPECT().RecordBookingOutcome(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			},
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordBookingConfirmedUseCase(db, repo).Execute(context.Background(), domain.BookingConfirmed{
					EventID: uuid.New(), BookingID: uuid.New(), TicketedEventID: uuid.New(), OccurredAt: now,
				})
			},
		},
		{
			name: "booking cancelled",
			expect: func(repo *mocks.MockRepository) *gomock.Call {
				return repo.EXPECT().RecordBookingOutcome(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
			},
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordBookingCancelledUseCase(db, repo).Execute(context.Background(), domain.BookingCancelled{
					EventID: uuid.New(), BookingID: uuid.New(), TicketedEventID: uuid.New(), OccurredAt: now,
				})
			},
		},
	}
}

func TestRecordingWritesInsideOneTransactionAndCommitsLast(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			db := &testsupport.FakeDB{}
			tc.expect(repo).DoAndReturn(func(_ context.Context, tx pgx.Tx, _ uuid.UUID, _ any) (bool, error) {
				if !db.IsCurrentTx(tx) {
					t.Errorf("the repository was handed a transaction the use case did not open")
				}
				db.Note("write")
				return false, nil
			})

			already, err := tc.execute(db, repo)

			if err != nil || already {
				t.Fatalf("got (%v, %v), want (false, nil)", already, err)
			}
			if got := db.Calls(); len(got) != 3 || got[0] != "begin" || got[1] != "write" || got[2] != "commit" {
				t.Fatalf("calls = %v, want [begin write commit]", got)
			}
		})
	}
}

func TestRecordingAnAlreadyProcessedEventStillCommitsAndReportsIt(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			db := &testsupport.FakeDB{}
			tc.expect(repo).Return(true, nil)

			already, err := tc.execute(db, repo)

			if err != nil || !already {
				t.Fatalf("got (%v, %v), want (true, nil)", already, err)
			}
			if !db.Committed() {
				t.Fatalf("calls = %v, want the transaction committed", db.Calls())
			}
		})
	}
}

func TestRecordingRollsBackAndReturnsTheRepositoryErrorUnchanged(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			db := &testsupport.FakeDB{}
			boom := &domain.RepositoryError{Err: errors.New("deadlock detected")}
			tc.expect(repo).Return(false, boom)

			_, err := tc.execute(db, repo)

			if err != error(boom) {
				t.Fatalf("err = %v, want the repository error untouched so the consumer can classify it", err)
			}
			if db.Committed() {
				t.Fatalf("calls = %v, a failed write must not commit", db.Calls())
			}
		})
	}
}

func TestRecordingFailsWithARepositoryErrorWhenTheTransactionCannotBeOpenedOrCommitted(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name+" begin", func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			db := &testsupport.FakeDB{BeginErr: errors.New("pool exhausted")}
			tc.expect(repo).Times(0)

			_, err := tc.execute(db, repo)

			var repoErr *domain.RepositoryError
			if !errors.As(err, &repoErr) {
				t.Fatalf("err = %v, want a RepositoryError", err)
			}
		})
		t.Run(tc.name+" commit", func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			db := &testsupport.FakeDB{CommitErr: errors.New("connection reset")}
			tc.expect(repo).Return(false, nil)

			already, err := tc.execute(db, repo)

			var repoErr *domain.RepositoryError
			if !errors.As(err, &repoErr) || already {
				t.Fatalf("got (%v, %v), want (false, RepositoryError)", already, err)
			}
		})
	}
}

func TestRegistrationAndLoginWithABadPayloadNeverOpenATransaction(t *testing.T) {
	tests := []struct {
		name    string
		execute func(db port.Transactor, repo port.Repository) (bool, error)
		wantErr error
	}{
		{
			name: "registration without an email",
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordUserRegistrationUseCase(db, repo).Execute(context.Background(),
					domain.UserCreated{EventID: uuid.New(), UserID: uuid.New(), CreatedAt: time.Now()})
			},
			wantErr: domain.ErrInvalidUserRegistration,
		},
		{
			name: "login without a time",
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordUserLoginUseCase(db, repo).Execute(context.Background(),
					domain.UserLoggedIn{EventID: uuid.New(), UserID: uuid.New(), Email: "ada@example.com"})
			},
			wantErr: domain.ErrInvalidUserLogin,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			db := &testsupport.FakeDB{}

			_, err := tc.execute(db, repo)

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if db.Begun() {
				t.Fatalf("a payload rejected by the domain must not pin a pooled connection")
			}
		})
	}
}

func TestBookingOutcomesAreRecordedAgainstTheTicketedEventWithTheRightStatus(t *testing.T) {
	bookingID, ticketedEventID, eventID := uuid.New(), uuid.New(), uuid.New()
	occurredAt := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		wantStatus string
		execute    func(db port.Transactor, repo port.Repository) (bool, error)
	}{
		{
			name:       "confirmed",
			wantStatus: domain.OutcomeConfirmed,
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordBookingConfirmedUseCase(db, repo).Execute(context.Background(), domain.BookingConfirmed{
					EventID: eventID, BookingID: bookingID, TicketedEventID: ticketedEventID, OccurredAt: occurredAt,
				})
			},
		},
		{
			name:       "cancelled",
			wantStatus: domain.OutcomeCancelled,
			execute: func(db port.Transactor, repo port.Repository) (bool, error) {
				return usecase.NewRecordBookingCancelledUseCase(db, repo).Execute(context.Background(), domain.BookingCancelled{
					EventID: eventID, BookingID: bookingID, TicketedEventID: ticketedEventID, OccurredAt: occurredAt,
				})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := mocks.NewMockRepository(gomock.NewController(t))
			repo.EXPECT().
				RecordBookingOutcome(gomock.Any(), gomock.Any(), eventID, gomock.Cond(func(o domain.BookingOutcome) bool {
					return o.Status == tc.wantStatus && o.BookingID == bookingID &&
						o.EventID == ticketedEventID && o.OccurredAt.Equal(occurredAt)
				})).
				Return(false, nil)

			if _, err := tc.execute(&testsupport.FakeDB{}, repo); err != nil {
				t.Fatalf("err = %v", err)
			}
		})
	}
}
