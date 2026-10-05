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

	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/app"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/config"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/db"
	"github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/internal/platform/logger"

	_ "github.com/buixuankhai1204/ticket-microservice-golang/services/event-service/docs"
)

//	@title			event-service API
//	@version		1.0
//	@description	Event catalogue: browse events and their seat maps, and create new events with their seats.
//	@BasePath		/api/v1

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
	db.RegisterPoolMetrics(pool)

	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	application := app.New(pool, cfg, log)
	reapHeldReservations := application.ReapHeldReservations

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           application.Router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	consumers := application.NewConsumers()
	for _, c := range consumers {
		defer func(c app.Consumer) { _ = c.Close() }(c)
	}

	var consumersWG sync.WaitGroup
	for _, c := range consumers {
		consumersWG.Add(1)
		go func(c app.Consumer) {
			defer consumersWG.Done()
			if err := c.Run(ctx); err != nil {
				log.Error("kafka consumer exited with error", "err", err.Error())
			}
		}(c)
	}

	var reaperWG sync.WaitGroup
	reaperWG.Add(1)
	go func() {
		defer reaperWG.Done()
		interval := time.Duration(cfg.SeatReaperIntervalSecs) * time.Second
		log.Info("seat reaper started", "interval_secs", cfg.SeatReaperIntervalSecs, "hold_timeout_secs", cfg.SeatHoldTimeoutSecs)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := reapHeldReservations.Execute(ctx)
				if err != nil {
					log.Error("seat reaper tick failed", "err", err.Error())
				} else if n > 0 {
					log.Warn("seat reaper released stale held reservations", "count", n)
				}
			}
		}
	}()

	serverErr := make(chan error, 1)
	go func() {
		log.Info("event-service listening", "port", cfg.Port)
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
	reaperWG.Wait()
	return nil
}
