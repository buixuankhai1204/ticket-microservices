package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

	consumer := kafkaconsumer.NewConsumer(kafkaconsumer.Config{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaUserEventsTopic,
		MaxAttempts: cfg.KafkaConsumerMaxAttempts,
	}, recordUserRegistration, log)
	defer func() { _ = consumer.Close() }()

	loginConsumer := kafkaconsumer.NewUserLoggedInConsumer(kafkaconsumer.UserLoggedInConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaUserEventsTopic,
		MaxAttempts: cfg.KafkaConsumerMaxAttempts,
	}, recordUserLogin, log)
	defer func() { _ = loginConsumer.Close() }()

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		if err := consumer.Run(ctx); err != nil {
			log.Error("kafka consumer exited with error", "err", err.Error())
		}
	}()

	loginConsumerDone := make(chan struct{})
	go func() {
		defer close(loginConsumerDone)
		if err := loginConsumer.Run(ctx); err != nil {
			log.Error("kafka login consumer exited with error", "err", err.Error())
		}
	}()

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

	<-consumerDone
	<-loginConsumerDone
	return nil
}
