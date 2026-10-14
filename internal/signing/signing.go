// Package signing signs webhook requests and verifies those signatures.
//
// The signature is hex(HMAC-SHA256(secret, "<timestamp>.<body>")), sent as
// "Webhook-Signature: v1=<hex>" together with "Webhook-Timestamp: <unix secs>".
// Including the timestamp lets receivers reject replayed requests.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	TimestampHeader = "Webhook-Timestamp"
	SignatureHeader = "Webhook-Signature"
)

// Sign returns the Webhook-Signature header value for body sent at ts.
func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks the signature and that the timestamp is within tolerance of now.
func Verify(secret, timestamp, signature string, body []byte, tolerance time.Duration, now time.Time) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errors.New("invalid timestamp")
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return errors.New("timestamp outside tolerance")
	}
	if !strings.HasPrefix(signature, "v1=") {
		return errors.New("unsupported signature version")
	}
	if !hmac.Equal([]byte(Sign(secret, ts, body)), []byte(signature)) {
		return errors.New("signature mismatch")
	}
	return nil
}
