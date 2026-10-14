// Package api implements the HTTP API.
package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/gnanreddy13/webhook-delivery/internal/store"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type API struct {
	store  *store.Store
	apiKey string
}

// NewRouter builds the HTTP handler. Every route except /healthz requires
// "Authorization: Bearer <apiKey>".
func NewRouter(s *store.Store, apiKey string) http.Handler {
	a := &API{store: s, apiKey: apiKey}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	r.Group(func(r chi.Router) {
		r.Use(a.requireAPIKey)
		r.Post("/endpoints", a.createEndpoint)
		r.Post("/events", a.createEvent)
		r.Get("/deliveries", a.listDeliveries)
		r.Get("/deliveries/{id}", a.getDelivery)
	})
	return r
}

func (a *API) requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(a.apiKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type createEndpointRequest struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
}

func (a *API) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if !decode(w, r, &req) {
		return
	}
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		writeError(w, http.StatusBadRequest, "url must be an absolute http(s) URL")
		return
	}
	if len(req.EventTypes) == 0 {
		writeError(w, http.StatusBadRequest, "event_types must not be empty")
		return
	}
	for _, t := range req.EventTypes {
		if strings.TrimSpace(t) == "" {
			writeError(w, http.StatusBadRequest, "event_types must not contain empty strings")
			return
		}
	}

	secret, err := newSecret()
	if err != nil {
		serverError(w, err)
		return
	}
	ep, err := a.store.CreateEndpoint(r.Context(), req.URL, req.EventTypes, secret)
	if err != nil {
		serverError(w, err)
		return
	}
	// The secret is only ever returned here, at creation time.
	writeJSON(w, http.StatusCreated, ep)
}

func (a *API) createEvent(w http.ResponseWriter, r *http.Request) {
	var ev store.Event
	if !decode(w, r, &ev) {
		return
	}
	if ev.ID == "" || ev.Type == "" {
		writeError(w, http.StatusBadRequest, "id and type are required")
		return
	}
	if len(ev.Payload) == 0 || string(ev.Payload) == "null" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	created, n, err := a.store.CreateEvent(r.Context(), ev)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":         ev.ID,
		"duplicate":  !created,
		"deliveries": n,
	})
}

func (a *API) listDeliveries(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	switch status {
	case "", "pending", "delivered", "dead":
	default:
		writeError(w, http.StatusBadRequest, "status must be pending, delivered or dead")
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
			return
		}
		limit = n
	}
	ds, err := a.store.ListDeliveries(r.Context(), status, limit)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": ds})
}

func (a *API) getDelivery(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid delivery id")
		return
	}
	d, err := a.store.GetDelivery(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "delivery not found")
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func newSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "whsec_" + hex.EncodeToString(b), nil
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func serverError(w http.ResponseWriter, err error) {
	slog.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}
