// Command receiver is a fake webhook consumer for demos and benchmarks.
//
//	POST /hook   verifies the signature (if RECEIVER_SECRET is set), then fails
//	             with HTTP 500 at RECEIVER_FAIL_RATE, otherwise returns 200.
//	POST /fail   always returns 500 (to demonstrate dead deliveries).
//	GET  /stats  counters as JSON.
package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gnanreddy13/webhook-delivery/internal/signing"
)

type stats struct {
	mu            sync.Mutex
	Requests      int `json:"requests"`
	OK            int `json:"ok"`
	InjectedFails int `json:"injected_failures"`
	BadSignatures int `json:"bad_signatures"`
	// UniqueDelivered counts distinct Webhook-Delivery-Id values we answered
	// 2xx for; DuplicateOK counts 2xx answers for an id we had already acked.
	UniqueDelivered int `json:"unique_delivered"`
	DuplicateOK     int `json:"duplicate_ok"`
	acked           map[string]bool
}

func main() {
	addr := getenv("RECEIVER_ADDR", "127.0.0.1:9090")
	secret := os.Getenv("RECEIVER_SECRET")
	failRate, err := strconv.ParseFloat(getenv("RECEIVER_FAIL_RATE", "0.3"), 64)
	if err != nil || failRate < 0 || failRate > 1 {
		log.Fatal("RECEIVER_FAIL_RATE must be a number between 0 and 1")
	}
	delay, err := time.ParseDuration(getenv("RECEIVER_DELAY", "0s"))
	if err != nil {
		log.Fatal("RECEIVER_DELAY: ", err)
	}

	st := &stats{acked: map[string]bool{}}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		time.Sleep(delay)

		st.mu.Lock()
		defer st.mu.Unlock()
		st.Requests++
		if secret != "" {
			err := signing.Verify(secret, r.Header.Get(signing.TimestampHeader),
				r.Header.Get(signing.SignatureHeader), body, 5*time.Minute, time.Now())
			if err != nil {
				st.BadSignatures++
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
		}
		if rand.Float64() < failRate {
			st.InjectedFails++
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		}
		st.OK++
		id := r.Header.Get("Webhook-Delivery-Id")
		if st.acked[id] {
			st.DuplicateOK++
		} else {
			st.acked[id] = true
			st.UniqueDelivered++
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("POST /fail", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		http.Error(w, "always failing", http.StatusInternalServerError)
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(st)
	})

	log.Printf("receiver listening on %s (fail rate %.2f, signature check %v)", addr, failRate, secret != "")
	log.Fatal(http.ListenAndServe(addr, mux))
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
