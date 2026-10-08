package app

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	httpadapter "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/http"
	kafkaconsumer "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/usecase"
)

type Consumer interface {
	Run(context.Context) error
	Close() error
}

type App struct {
	Router               http.Handler
	ReapHeldReservations *usecase.ReapHeldReservationsUseCase

	cfg          config.Config
	log          logger.Logger
	reserveSeat  *usecase.ReserveSeatUseCase
	finalizeSeat *usecase.FinalizeSeatUseCase
	releaseSeat  *usecase.ReleaseSeatUseCase
}

func New(pool *pgxpool.Pool, cfg config.Config, log logger.Logger) *App {
	repo := postgres.New()
	listEvents := usecase.NewListEventsUseCase(pool, repo)
	getEvent := usecase.NewGetEventUseCase(pool, repo)
	listEventSeats := usecase.NewListEventSeatsUseCase(pool, repo)
	createNewEvent := usecase.NewCreateNewEventUseCase(pool, repo)
	reserveSeat := usecase.NewReserveSeatUseCase(pool, repo)
	finalizeSeat := usecase.NewFinalizeSeatUseCase(pool, repo)
	releaseSeat := usecase.NewReleaseSeatUseCase(pool, repo)
	reapHeldReservations := usecase.NewReapHeldReservationsUseCase(pool, repo, cfg.SeatHoldTimeoutSecs)

	handler := httpadapter.NewHandler(listEvents, getEvent, listEventSeats, createNewEvent)
	health := httpadapter.NewHealthHandler(pool)
	router := httpadapter.NewRouter(handler, health,
		httpadapter.RequestID(),
		httpadapter.AccessLog(log),
		httpadapter.Metrics(),
	)

	return &App{
		Router:               router,
		ReapHeldReservations: reapHeldReservations,
		cfg:                  cfg,
		log:                  log,
		reserveSeat:          reserveSeat,
		finalizeSeat:         finalizeSeat,
		releaseSeat:          releaseSeat,
	}
}

func (a *App) NewConsumers() []Consumer {
	kafkaCfg := kafkaconsumer.Config{
		Brokers:     a.cfg.KafkaBrokers,
		Topic:       a.cfg.KafkaBookingEventsTopic,
		MaxAttempts: a.cfg.KafkaConsumerMaxAttempts,
	}
	suffix := a.cfg.KafkaGroupSuffix
	return []Consumer{
		kafkaconsumer.NewConsumer(kafkaCfg, grouped(kafkaconsumer.BookingRequestedSpec(a.reserveSeat), suffix), a.log),
		kafkaconsumer.NewConsumer(kafkaCfg, grouped(kafkaconsumer.BookingConfirmedSpec(a.finalizeSeat), suffix), a.log),
		kafkaconsumer.NewConsumer(kafkaCfg, grouped(kafkaconsumer.BookingCancelledSpec(a.releaseSeat), suffix), a.log),
	}
}

func grouped[E any](spec kafkaconsumer.EventSpec[E], suffix string) kafkaconsumer.EventSpec[E] {
	spec.Group += suffix
	return spec
}
