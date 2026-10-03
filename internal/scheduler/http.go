package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

func Handler(s *Scheduler) http.Handler {
	mux := http.NewServeMux()
	var ids atomic.Uint64
	mux.HandleFunc("POST /generate", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Prompt    string `json:"prompt"`
			MaxTokens int    `json:"max_tokens"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			http.Error(w, "expected one JSON object", http.StatusBadRequest)
			return
		}
		req := NewRequest(r.Context(), strconv.FormatUint(ids.Add(1), 10), input.Prompt, input.MaxTokens, s.config.OutputBuffer)
		defer req.Cancel()
		if err := s.ValidateRequest(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.Submit(r.Context(), req); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("X-Request-ID", req.ID)
		controller := http.NewResponseController(w)
		writeEvent := func(event string, value any) error {
			if err := controller.SetWriteDeadline(time.Now().Add(s.config.StallTimeout)); err != nil {
				return err
			}
			payload, err := json.Marshal(value)
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload); err != nil {
				return err
			}
			return controller.Flush()
		}
		if err := controller.Flush(); err != nil {
			return
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case token, ok := <-req.Out:
				if !ok {
					_ = writeEvent("done", <-req.Result)
					return
				}
				if err := writeEvent("token", token); err != nil {
					return
				}
			}
		}
	})
	view := func(trace bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			v, err := s.Snapshot(r.Context(), trace)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if trace {
				_ = json.NewEncoder(w).Encode(v)
			} else {
				_ = json.NewEncoder(w).Encode(v.Metrics)
			}
		}
	}
	mux.HandleFunc("GET /metrics", view(false))
	mux.HandleFunc("GET /trace", view(true))
	return mux
}
