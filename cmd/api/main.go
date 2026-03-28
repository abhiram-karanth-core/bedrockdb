package main

import (
	"bedrockdb/internal/db"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	dir := "./data"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	database, err := db.Open(dir)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer database.Close()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	h := &handler{db: database}

	r.Put("/key/{key}", h.put)
	r.Get("/key/{key}", h.get)
	r.Delete("/key/{key}", h.delete)
	r.Get("/range", h.rangeQuery)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig

		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	log.Printf("bedrockdb listening on :8080, data dir: %s", dir)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}

type handler struct {
	db *db.DB
}

// PUT /key/{key}
// Body: {"value": "..."}
func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if err := h.db.Put(key, body.Value); err != nil {
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": body.Value})
}

// GET /key/{key}
func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")

	val, found, err := h.db.Get(key)
	if err != nil {
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

// DELETE /key/{key}
func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")

	if err := h.db.Delete(key); err != nil {
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"deleted": key})
}

// GET /range?start=a&end=z
func (h *handler) rangeQuery(w http.ResponseWriter, r *http.Request) {
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")

	if start == "" || end == "" {
		http.Error(w, `{"error":"start and end query params required"}`, http.StatusBadRequest)
		return
	}
	if start > end {
		http.Error(w, `{"error":"start must be <= end"}`, http.StatusBadRequest)
		return
	}

	entries, err := h.db.Range(start, end)
	if err != nil {
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}