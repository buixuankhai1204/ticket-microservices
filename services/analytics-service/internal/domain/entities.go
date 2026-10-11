package domain

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	OutcomeConfirmed = "confirmed"
	OutcomeCancelled = "cancelled"
)

type BookingOutcome struct {
	ID         uuid.UUID
	BookingID  uuid.UUID
	EventID    uuid.UUID
	Status     string
	OccurredAt time.Time
	RecordedAt time.Time
}

func NewBookingOutcome(bookingID, eventID uuid.UUID, status string, occurredAt time.Time) (*BookingOutcome, error) {
	if status != OutcomeConfirmed && status != OutcomeCancelled {
		return nil, ErrInvalidOutcomeStatus
	}
	return &BookingOutcome{
		ID:         uuid.New(),
		BookingID:  bookingID,
		EventID:    eventID,
		Status:     status,
		OccurredAt: occurredAt,
		RecordedAt: time.Now().UTC(),
	}, nil
}

type EventBookingStats struct {
	EventID   uuid.UUID
	Confirmed int64
	Cancelled int64
}

type UserRegistration struct {
	UserID       uuid.UUID
	Email        string
	RegisteredAt time.Time
	RecordedAt   time.Time
}

func NewUserRegistration(userID uuid.UUID, email string, registeredAt time.Time) (*UserRegistration, error) {
	if email == "" || !strings.Contains(email, "@") {
		return nil, ErrInvalidUserRegistration
	}
	return &UserRegistration{
		UserID:       userID,
		Email:        email,
		RegisteredAt: registeredAt,
		RecordedAt:   time.Now().UTC(),
	}, nil
}

type UserLogin struct {
	UserID     uuid.UUID
	Email      string
	LoggedInAt time.Time
	RecordedAt time.Time
}

func NewUserLogin(userID uuid.UUID, email string, loggedInAt time.Time) (*UserLogin, error) {
	if email == "" || !strings.Contains(email, "@") {
		return nil, ErrInvalidUserLogin
	}
	if loggedInAt.IsZero() {
		return nil, ErrInvalidUserLogin
	}
	return &UserLogin{
		UserID:     userID,
		Email:      email,
		LoggedInAt: loggedInAt,
		RecordedAt: time.Now().UTC(),
	}, nil
}

const (
	ProductOrderStatusPlaced    = "placed"
	ProductOrderStatusCancelled = "cancelled"
)

type ProductOrderOutcome struct {
	OrderID         uuid.UUID
	BookingID       uuid.UUID
	UserID          uuid.UUID
	TicketedEventID uuid.UUID
	Currency        string
	TotalMinor      int64
	ItemCount       int
	Status          string
	PlacedAt        *time.Time
	CancelledAt     *time.Time
	CancelReason    string
	RecordedAt      time.Time
}

func validProductOrderIdentity(currency string, totalMinor int64, itemCount int) bool {
	if len(currency) != 3 || strings.ToUpper(currency) != currency {
		return false
	}
	return totalMinor >= 0 && itemCount >= 0
}

func NewProductOrderPlacedOutcome(ev ProductOrderPlaced) (*ProductOrderOutcome, error) {
	if !validProductOrderIdentity(ev.Currency, ev.TotalMinor, ev.ItemCount) || ev.PlacedAt.IsZero() {
		return nil, ErrInvalidProductOrder
	}
	placedAt := ev.PlacedAt
	return &ProductOrderOutcome{
		OrderID:         ev.OrderID,
		BookingID:       ev.BookingID,
		UserID:          ev.UserID,
		TicketedEventID: ev.TicketedEventID,
		Currency:        ev.Currency,
		TotalMinor:      ev.TotalMinor,
		ItemCount:       ev.ItemCount,
		Status:          ProductOrderStatusPlaced,
		PlacedAt:        &placedAt,
		RecordedAt:      time.Now().UTC(),
	}, nil
}

func NewProductOrderCancelledOutcome(ev ProductOrderCancelled) (*ProductOrderOutcome, error) {
	if !validProductOrderIdentity(ev.Currency, ev.TotalMinor, ev.ItemCount) || ev.CancelledAt.IsZero() || ev.Reason == "" {
		return nil, ErrInvalidProductOrder
	}
	cancelledAt := ev.CancelledAt
	return &ProductOrderOutcome{
		OrderID:         ev.OrderID,
		BookingID:       ev.BookingID,
		UserID:          ev.UserID,
		TicketedEventID: ev.TicketedEventID,
		Currency:        ev.Currency,
		TotalMinor:      ev.TotalMinor,
		ItemCount:       ev.ItemCount,
		Status:          ProductOrderStatusCancelled,
		CancelledAt:     &cancelledAt,
		CancelReason:    ev.Reason,
		RecordedAt:      time.Now().UTC(),
	}, nil
}
