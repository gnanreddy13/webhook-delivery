// Package config loads settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL string
	ListenAddr  string
	APIKey      string

	WorkerCount     int
	MaxAttempts     int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	DeliveryTimeout time.Duration
	LeaseDuration   time.Duration
	PollInterval    time.Duration
	ClaimBatchSize  int
}

// Load reads the configuration from the environment, applying defaults.
func Load() (Config, error) {
	c := Config{
		DatabaseURL: getenv("DATABASE_URL", "postgres://webhook:change-me-local-only@127.0.0.1:5432/webhook?sslmode=disable"),
		ListenAddr:  getenv("LISTEN_ADDR", "127.0.0.1:8080"),
		APIKey:      os.Getenv("API_KEY"),
	}
	var err error
	if c.WorkerCount, err = intEnv("WORKER_COUNT", 4); err != nil {
		return c, err
	}
	if c.MaxAttempts, err = intEnv("MAX_ATTEMPTS", 8); err != nil {
		return c, err
	}
	if c.ClaimBatchSize, err = intEnv("CLAIM_BATCH_SIZE", 1); err != nil {
		return c, err
	}
	if c.BackoffBase, err = durationEnv("BACKOFF_BASE", time.Second); err != nil {
		return c, err
	}
	if c.BackoffMax, err = durationEnv("BACKOFF_MAX", 10*time.Minute); err != nil {
		return c, err
	}
	if c.DeliveryTimeout, err = durationEnv("DELIVERY_TIMEOUT", 5*time.Second); err != nil {
		return c, err
	}
	if c.LeaseDuration, err = durationEnv("LEASE_DURATION", 30*time.Second); err != nil {
		return c, err
	}
	if c.PollInterval, err = durationEnv("POLL_INTERVAL", 250*time.Millisecond); err != nil {
		return c, err
	}

	switch {
	case c.APIKey == "":
		return c, errors.New("API_KEY must be set")
	case c.WorkerCount < 0:
		return c, errors.New("WORKER_COUNT must be >= 0")
	case c.MaxAttempts < 1:
		return c, errors.New("MAX_ATTEMPTS must be >= 1")
	case c.ClaimBatchSize < 1:
		return c, errors.New("CLAIM_BATCH_SIZE must be >= 1")
	case c.LeaseDuration < c.DeliveryTimeout+2*time.Second:
		return c, errors.New("LEASE_DURATION must be at least DELIVERY_TIMEOUT + 2s")
	}
	return c, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
