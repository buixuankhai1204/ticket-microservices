package app

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	httpadapter "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/http"
	kafkaconsumer "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/logger"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"
)

type Consumer interface {
	Run(context.Context) error
	Close() error
}

type route struct {
	topic    string
	consumer func(cfg kafkaconsumer.Config, log logger.Logger) Consumer
	handler  func(dlq kafkaconsumer.DeadLetters, policy kafkaconsumer.RetryPolicy, log logger.Logger) kafkaconsumer.Handler
}

func bind[E any](topic, groupSuffix string, spec kafkaconsumer.EventSpec[E]) route {
	spec.Group += groupSuffix
	return route{
		topic: topic,
		consumer: func(cfg kafkaconsumer.Config, log logger.Logger) Consumer {
			cfg.Topic = topic
			return kafkaconsumer.NewConsumer(cfg, spec, log)
		},
		handler: func(dlq kafkaconsumer.DeadLetters, policy kafkaconsumer.RetryPolicy, log logger.Logger) kafkaconsumer.Handler {
			return kafkaconsumer.NewProcessor(spec, dlq, policy, log)
		},
	}
}

type App struct {
	Router http.Handler

	cfg    config.Config
	log    logger.Logger
	routes []route
}

func New(pool *pgxpool.Pool, cfg config.Config, log logger.Logger) *App {
	repo := postgres.New()
	getEventStats := usecase.NewGetEventStatsUseCase(pool, repo)
	getUserRegistration := usecase.NewGetUserRegistrationUseCase(pool, repo)
	recordUserRegistration := usecase.NewRecordUserRegistrationUseCase(pool, repo)
	recordUserLogin := usecase.NewRecordUserLoginUseCase(pool, repo)
	recordBookingConfirmed := usecase.NewRecordBookingConfirmedUseCase(pool, repo)
	recordBookingCancelled := usecase.NewRecordBookingCancelledUseCase(pool, repo)

	handler := httpadapter.NewHandler(getEventStats, getUserRegistration)
	health := httpadapter.NewHealthHandler(pool)
	router := httpadapter.NewRouter(handler, health,
		httpadapter.RequestID(),
		httpadapter.AccessLog(log),
		httpadapter.Metrics(),
	)

	suffix := cfg.KafkaGroupSuffix
	return &App{
		Router: router,
		cfg:    cfg,
		log:    log,
		routes: []route{
			bind(cfg.KafkaUserEventsTopic, suffix, kafkaconsumer.UserCreatedSpec(recordUserRegistration)),
			bind(cfg.KafkaUserEventsTopic, suffix, kafkaconsumer.UserLoggedInSpec(recordUserLogin)),
			bind(cfg.KafkaBookingEventsTopic, suffix, kafkaconsumer.BookingConfirmedSpec(recordBookingConfirmed)),
			bind(cfg.KafkaBookingEventsTopic, suffix, kafkaconsumer.BookingCancelledSpec(recordBookingCancelled)),
		},
	}
}

func (a *App) NewConsumers() []Consumer {
	consumers := make([]Consumer, 0, len(a.routes))
	for _, r := range a.routes {
		consumers = append(consumers, r.consumer(kafkaconsumer.Config{
			Brokers:     a.cfg.KafkaBrokers,
			MaxAttempts: a.cfg.KafkaConsumerMaxAttempts,
		}, a.log))
	}
	return consumers
}

func (a *App) NewInbox(dlq kafkaconsumer.DeadLetters, policy kafkaconsumer.RetryPolicy) kafkaconsumer.Inbox {
	inbox := make(kafkaconsumer.Inbox, 0, len(a.routes))
	for _, r := range a.routes {
		inbox = append(inbox, kafkaconsumer.Route{Topic: r.topic, Handler: r.handler(dlq, policy, a.log)})
	}
	return inbox
}
