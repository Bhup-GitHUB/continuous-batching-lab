package loadgen

import (
	"context"
	"encoding/csv"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"continuous-batching-lab/internal/clock"
	"continuous-batching-lab/internal/model"
	"continuous-batching-lab/internal/scheduler"
)

func TestReadStreamsAndDisconnect(t *testing.T) {
	sc := scheduler.DefaultConfig()
	s, err := scheduler.New(sc, clock.Real{}, &model.Fake{Clock: clock.Real{}, Costs: model.Costs{StepBase: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); s.Run(ctx) }()
	server := httptest.NewServer(scheduler.Handler(s))
	t.Cleanup(func() { cancel(); server.Close(); <-finished })
	for _, disconnect := range []bool{false, true} {
		c := DefaultConfig()
		c.URL = server.URL
		c.RPS = 100
		c.Duration = 40 * time.Millisecond
		c.ShortTokens = 4
		c.LongTokens = 8
		c.Timeout = time.Second
		c.SlowFraction = 1
		c.SlowDelay = time.Millisecond
		if disconnect {
			c.DisconnectFraction = 1
		}
		report, err := Run(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		if report.Summary.Requests != 4 || report.Summary.Errors != 0 || report.Summary.TTFT.Samples != 4 {
			t.Fatalf("summary=%+v", report.Summary)
		}
		if disconnect && report.Summary.Disconnected != 4 {
			t.Fatal(report.Summary)
		}
		if !disconnect && (report.Summary.Completed != 4 || report.Summary.Gaps.Samples != report.Summary.Tokens-4) {
			t.Fatal(report.Summary)
		}
		path := filepath.Join(t.TempDir(), "requests.csv")
		if err := WriteCSV(path, report.Records); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := csv.NewReader(f).ReadAll()
		_ = f.Close()
		if err != nil || len(rows) != 5 || rows[0][11] != "gaps_ms" {
			t.Fatalf("CSV rows=%v err=%v", rows, err)
		}
	}
}
