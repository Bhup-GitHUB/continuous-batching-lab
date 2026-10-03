package scheduler

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"continuous-batching-lab/internal/clock"
	"continuous-batching-lab/internal/model"
)

func liveServer(t *testing.T) (*Scheduler, *httptest.Server) {
	t.Helper()
	c := DefaultConfig()
	c.MaxActive = 1
	c.OutputBuffer = 1
	s, err := New(c, clock.Real{}, &model.Fake{Clock: clock.Real{}, Costs: model.Costs{StepBase: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); s.Run(ctx) }()
	server := httptest.NewServer(Handler(s))
	t.Cleanup(func() { cancel(); server.Close(); <-finished })
	return s, server
}
func TestHTTPStream(t *testing.T) {
	_, server := liveServer(t)
	resp, err := server.Client().Post(server.URL+"/generate", "application/json", strings.NewReader(`{"prompt":"hello world","max_tokens":3}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal(resp.Status)
	}
	if strings.Count(string(body), "event: token") != 3 || !strings.Contains(string(body), `event: done`+"\n"+`data: {"reason":"completed","tokens":3}`) {
		t.Fatalf("stream=%s", body)
	}
	for _, input := range []string{`{"max_tokens":0}`, `{"prompt":"p","max_tokens":20000}`, `{"max_tokens":1} {}`, `{"extra":true,"max_tokens":1}`} {
		r, err := server.Client().Post(server.URL+"/generate", "application/json", strings.NewReader(input))
		if err != nil {
			t.Fatal(err)
		}
		_ = r.Body.Close()
		if r.StatusCode != 400 {
			t.Fatalf("%s: %s", input, r.Status)
		}
	}
	r, err := server.Client().Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var v map[string]any
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	if v["active"].(float64) != 0 || v["kv_budget_used"].(float64) != 0 {
		t.Fatal(v)
	}
}
func TestHTTPDisconnectCancels(t *testing.T) {
	s, server := liveServer(t)
	resp, err := server.Client().Post(server.URL+"/generate", "application/json", strings.NewReader(`{"prompt":"hello","max_tokens":1000}`))
	if err != nil {
		t.Fatal(err)
	}
	scan := bufio.NewScanner(resp.Body)
	for scan.Scan() {
		if strings.HasPrefix(scan.Text(), "data:") {
			break
		}
	}
	_ = resp.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		v, err := s.Snapshot(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		if v.Metrics.Totals.Cancelled == 1 {
			if v.Metrics.Active != 0 || v.Metrics.KVUsed != 0 {
				t.Fatal(v)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("disconnect did not cancel")
		case <-time.After(time.Millisecond):
		}
	}
}
