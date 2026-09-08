package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	httpadapter "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/http"
	kafkaconsumer "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/messaging/kafka"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/adapter/repository/postgres"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/db"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/platform/logger"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/internal/usecase"

	_ "github.com/buixuankhai1204/ticket-microservice-golang/services/analytics-service/docs"
)

//	@title						analytics-service API
//	@version					1.0
//	@description				Read-only projections built from saga events (user registrations, event booking stats).
//	@BasePath					/api/v1
//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization

func main() {
	log := logger.New()
	if err := run(log); err != nil {
		log.Error("fatal", "err", err.Error())
		os.Exit(1)
	}
}

func run(log logger.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

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
	)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	userKafkaCfg := kafkaconsumer.Config{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaUserEventsTopic,
		MaxAttempts: cfg.KafkaConsumerMaxAttempts,
	}
	bookingKafkaCfg := kafkaconsumer.Config{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaBookingEventsTopic,
		MaxAttempts: cfg.KafkaConsumerMaxAttempts,
	}
	consumers := []consumerRunner{
		kafkaconsumer.NewConsumer(userKafkaCfg, kafkaconsumer.UserCreatedSpec(recordUserRegistration), log),
		kafkaconsumer.NewConsumer(userKafkaCfg, kafkaconsumer.UserLoggedInSpec(recordUserLogin), log),
		kafkaconsumer.NewConsumer(bookingKafkaCfg, kafkaconsumer.BookingConfirmedSpec(recordBookingConfirmed), log),
		kafkaconsumer.NewConsumer(bookingKafkaCfg, kafkaconsumer.BookingCancelledSpec(recordBookingCancelled), log),
	}
	for _, c := range consumers {
		defer func(c consumerRunner) { _ = c.Close() }(c)
	}

	var consumersWG sync.WaitGroup
	for _, c := range consumers {
		consumersWG.Add(1)
		go func(c consumerRunner) {
			defer consumersWG.Done()
			if err := c.Run(ctx); err != nil {
				log.Error("kafka consumer exited with error", "err", err.Error())
			}
		}(c)
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("analytics-service listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received, draining in-flight requests")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}

	consumersWG.Wait()
	return nil
}

type consumerRunner interface {
	Run(context.Context) error
	Close() error
}
