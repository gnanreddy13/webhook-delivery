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

	if cfg.AllowPrivateDestinations {
		slog.Warn("ALLOW_PRIVATE_DESTINATIONS=true: SSRF protection is OFF; webhooks may target localhost and private networks")
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
		Handler:           api.NewRouter(st, api.Options{APIKey: cfg.APIKey, AllowPrivateDestinations: cfg.AllowPrivateDestinations}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
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

		AllowPrivateDestinations: cfg.AllowPrivateDestinations,
	}, st)
	workersDone := make(chan struct{})
	go func() {
		workers.Run(ctx)
		close(workersDone)
	}()

	errc := make(chan error, 1)
	go func() {
		tls := cfg.TLSCertFile != ""
		slog.Info("listening", "addr", cfg.ListenAddr, "tls", tls, "workers", cfg.WorkerCount)
		var err error
		if tls {
			err = srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
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
