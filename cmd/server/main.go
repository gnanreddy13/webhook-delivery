// Command server runs the webhook API and the delivery workers.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gnanreddy13/webhook-delivery/internal/api"
	"github.com/gnanreddy13/webhook-delivery/internal/config"
	"github.com/gnanreddy13/webhook-delivery/internal/db"
	"github.com/gnanreddy13/webhook-delivery/internal/store"
	"github.com/gnanreddy13/webhook-delivery/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	if cfg.APIKey == "change-me" {
		slog.Warn("API_KEY is the placeholder from .env.example; set a real one (openssl rand -hex 32)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	st := store.New(pool)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewRouter(st, cfg.APIKey),
		ReadHeaderTimeout: 5 * time.Second,
	}

	workers := worker.New(worker.Config{
		Workers:      cfg.WorkerCount,
		BatchSize:    cfg.ClaimBatchSize,
		MaxAttempts:  cfg.MaxAttempts,
		BackoffBase:  cfg.BackoffBase,
		BackoffMax:   cfg.BackoffMax,
		Timeout:      cfg.DeliveryTimeout,
		Lease:        cfg.LeaseDuration,
		PollInterval: cfg.PollInterval,
	}, st)
	workersDone := make(chan struct{})
	go func() {
		workers.Run(ctx)
		close(workersDone)
	}()

	errc := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.ListenAddr, "workers", cfg.WorkerCount)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	var runErr error
	select {
	case runErr = <-errc:
		stop() // also stop the workers
	case <-ctx.Done():
	}

	// Stop accepting requests, then wait for workers to finish their
	// in-flight attempt (bounded by DELIVERY_TIMEOUT).
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = err
	}
	<-workersDone
	slog.Info("stopped")
	return runErr
}
