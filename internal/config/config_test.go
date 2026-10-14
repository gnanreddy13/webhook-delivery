package config

import (
	"strings"
	"testing"
)

func TestLoadValidation(t *testing.T) {
	strongKey := strings.Repeat("k", 32)
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"missing database url", map[string]string{"DATABASE_URL": ""}, "DATABASE_URL"},
		{"missing key", map[string]string{"API_KEY": ""}, "API_KEY must be set"},
		{"missing encryption key", map[string]string{"SECRET_ENCRYPTION_KEY": ""}, "SECRET_ENCRYPTION_KEY"},
		{"short key", map[string]string{"API_KEY": "change-me"}, "at least 32"},
		{"public http refused", map[string]string{"LISTEN_ADDR": "0.0.0.0:8080"}, "refusing plain HTTP"},
		{"public http behind proxy", map[string]string{"LISTEN_ADDR": "0.0.0.0:8080", "ALLOW_INSECURE_HTTP": "true"}, ""},
		{"public https", map[string]string{"LISTEN_ADDR": ":8443", "TLS_CERT_FILE": "c.pem", "TLS_KEY_FILE": "k.pem"}, ""},
		{"half tls config", map[string]string{"TLS_CERT_FILE": "c.pem"}, "both TLS_CERT_FILE"},
		{"loopback http", map[string]string{"LISTEN_ADDR": "127.0.0.1:8080"}, ""},
		{"ipv6 loopback http", map[string]string{"LISTEN_ADDR": "[::1]:8080"}, ""},
		{"bad bool", map[string]string{"ALLOW_PRIVATE_DESTINATIONS": "maybe"}, "ALLOW_PRIVATE_DESTINATIONS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://localhost/x")
			t.Setenv("API_KEY", strongKey)
			t.Setenv("SECRET_ENCRYPTION_KEY", strings.Repeat("ab", 32))
			t.Setenv("LISTEN_ADDR", "")
			t.Setenv("TLS_CERT_FILE", "")
			t.Setenv("TLS_KEY_FILE", "")
			t.Setenv("ALLOW_INSECURE_HTTP", "")
			t.Setenv("ALLOW_PRIVATE_DESTINATIONS", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
