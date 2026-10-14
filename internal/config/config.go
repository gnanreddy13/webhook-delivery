// Package config loads settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL string
	DBMaxConns  int
	ListenAddr  string
	APIKey      string

	// TLS: serve HTTPS directly when both files are set. Plain HTTP on a
	// non-loopback address is refused unless AllowInsecureHTTP is set (for
	// running behind a TLS-terminating reverse proxy).
	TLSCertFile       string
	TLSKeyFile        string
	AllowInsecureHTTP bool

	// AllowPrivateDestinations disables SSRF protection so webhooks can be
	// sent to localhost/private networks. Local development only.
	AllowPrivateDestinations bool

	// Endpoint secrets are encrypted at rest with this key (64 hex chars).
	// During a rotation, the old key goes in SecretEncryptionKeyPrevious.
	SecretEncryptionKey         string
	SecretEncryptionKeyPrevious string

	WorkerCount     int
	MaxAttempts     int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	DeliveryTimeout time.Duration
	LeaseDuration   time.Duration
	PollInterval    time.Duration
	ClaimBatchSize  int

	EndpointMaxInflight int
	BreakerThreshold    int
	BreakerCooldown     time.Duration
	BreakerMaxCooldown  time.Duration
}

// Load reads the configuration from the environment, applying defaults.
func Load() (Config, error) {
	c := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		ListenAddr:  getenv("LISTEN_ADDR", "127.0.0.1:8080"),
		APIKey:      os.Getenv("API_KEY"),
		TLSCertFile: os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:  os.Getenv("TLS_KEY_FILE"),

		SecretEncryptionKey:         os.Getenv("SECRET_ENCRYPTION_KEY"),
		SecretEncryptionKeyPrevious: os.Getenv("SECRET_ENCRYPTION_KEY_PREVIOUS"),
	}
	var err error
	if c.AllowInsecureHTTP, err = boolEnv("ALLOW_INSECURE_HTTP"); err != nil {
		return c, err
	}
	if c.AllowPrivateDestinations, err = boolEnv("ALLOW_PRIVATE_DESTINATIONS"); err != nil {
		return c, err
	}
	if c.DBMaxConns, err = intEnv("DB_MAX_CONNS", 20); err != nil {
		return c, err
	}
	if c.EndpointMaxInflight, err = intEnv("ENDPOINT_MAX_INFLIGHT", 4); err != nil {
		return c, err
	}
	if c.BreakerThreshold, err = intEnv("BREAKER_THRESHOLD", 5); err != nil {
		return c, err
	}
	if c.BreakerCooldown, err = durationEnv("BREAKER_COOLDOWN", 30*time.Second); err != nil {
		return c, err
	}
	if c.BreakerMaxCooldown, err = durationEnv("BREAKER_MAX_COOLDOWN", 10*time.Minute); err != nil {
		return c, err
	}
	if c.WorkerCount, err = intEnv("WORKER_COUNT", 16); err != nil {
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
	case c.DatabaseURL == "":
		return c, errors.New("DATABASE_URL must be set")
	case c.APIKey == "":
		return c, errors.New("API_KEY must be set (run ./scripts/init-env.sh to generate one)")
	case c.SecretEncryptionKey == "":
		return c, errors.New("SECRET_ENCRYPTION_KEY must be set (run ./scripts/init-env.sh to generate one)")
	case len(c.APIKey) < 32:
		return c, errors.New("API_KEY must be at least 32 characters (e.g. openssl rand -hex 32)")
	case (c.TLSCertFile == "") != (c.TLSKeyFile == ""):
		return c, errors.New("set both TLS_CERT_FILE and TLS_KEY_FILE, or neither")
	case c.TLSCertFile == "" && !c.AllowInsecureHTTP && !isLoopback(c.ListenAddr):
		return c, fmt.Errorf("refusing plain HTTP on non-loopback LISTEN_ADDR %q: set TLS_CERT_FILE/TLS_KEY_FILE, "+
			"or ALLOW_INSECURE_HTTP=true if a reverse proxy terminates TLS in front of this server", c.ListenAddr)
	case c.DBMaxConns < 2:
		return c, errors.New("DB_MAX_CONNS must be >= 2")
	case c.WorkerCount < 0:
		return c, errors.New("WORKER_COUNT must be >= 0")
	case c.MaxAttempts < 1:
		return c, errors.New("MAX_ATTEMPTS must be >= 1")
	case c.EndpointMaxInflight < 1:
		return c, errors.New("ENDPOINT_MAX_INFLIGHT must be >= 1")
	case c.BreakerThreshold < 0:
		return c, errors.New("BREAKER_THRESHOLD must be >= 0 (0 disables the circuit breaker)")
	case c.ClaimBatchSize < 1:
		return c, errors.New("CLAIM_BATCH_SIZE must be >= 1")
	case c.LeaseDuration < c.DeliveryTimeout+2*time.Second:
		return c, errors.New("LEASE_DURATION must be at least DELIVERY_TIMEOUT + 2s")
	}
	return c, nil
}

// isLoopback reports whether a listen address only accepts local connections.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

func boolEnv(key string) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
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
