package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"continuous-batching-lab/internal/metrics"
	"continuous-batching-lab/internal/model"
	"continuous-batching-lab/internal/scheduler"
)

type Config struct {
	URL, Prompt                                    string
	RPS                                            float64
	Duration, Timeout                              time.Duration
	ShortTokens, LongTokens                        int
	LongFraction, SlowFraction, DisconnectFraction float64
	SlowDelay                                      time.Duration
	DisconnectAfter, MaxInflight                   int
	Seed                                           int64
}

func DefaultConfig() Config {
	return Config{URL: "http://localhost:8080", Prompt: "Explain continuous batching in simple terms", RPS: 10, Duration: 60 * time.Second, Timeout: 60 * time.Second, ShortTokens: 16, LongTokens: 128, LongFraction: .25, SlowFraction: 0, DisconnectFraction: 0, SlowDelay: 100 * time.Millisecond, DisconnectAfter: 2, MaxInflight: 10000, Seed: 1}
}
func (c Config) Validate() error {
	if math.IsNaN(c.RPS) || math.IsInf(c.RPS, 0) || c.RPS <= 0 || c.RPS > 1e6 || c.Duration <= 0 || c.Timeout <= 0 || c.ShortTokens < 1 || c.LongTokens < 1 || c.SlowDelay < 0 || c.DisconnectAfter < 1 || c.MaxInflight < 1 {
		return errors.New("rate, durations, token counts, disconnect threshold and inflight limit must be positive")
	}
	for _, f := range []float64{c.LongFraction, c.SlowFraction, c.DisconnectFraction} {
		if math.IsNaN(f) || f < 0 || f > 1 {
			return errors.New("fractions must be in [0, 1]")
		}
	}
	return nil
}

type Record struct {
	ID                            int
	RequestID                     string
	MaxTokens, Tokens             int
	Slow, Disconnect              bool
	StartMS, TTFTMS, CompletionMS float64
	GapsMS                        []float64
	Status, Error                 string
}
type Summary struct {
	RPS                                                        float64
	Requests, Completed, Disconnected, Errors, Skipped, Tokens int
	ElapsedSeconds, TokensPerSecond                            float64
	TTFT, Completion, Gaps                                     metrics.Percentiles
}
type Report struct {
	Summary Summary
	Records []Record
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func Run(ctx context.Context, c Config) (Report, error) {
	if err := c.Validate(); err != nil {
		return Report{}, err
	}
	transport := &http.Transport{MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	rng := rand.New(rand.NewSource(c.Seed))
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var records []Record
	inflight := make(chan struct{}, c.MaxInflight)
	skipped := 0
	for i := 0; ; i++ {
		offset := time.Duration(float64(i) / c.RPS * float64(time.Second))
		if offset >= c.Duration {
			break
		}
		if err := wait(ctx, time.Until(start.Add(offset))); err != nil {
			break
		}
		tokens := c.ShortTokens
		if rng.Float64() < c.LongFraction {
			tokens = c.LongTokens
		}
		slow, disconnect := rng.Float64() < c.SlowFraction, rng.Float64() < c.DisconnectFraction
		select {
		case inflight <- struct{}{}:
		default:
			skipped++
			continue
		}
		wg.Add(1)
		go func(id, n int, slow, disconnect bool) {
			defer wg.Done()
			defer func() { <-inflight }()
			r := generate(ctx, client, c, start, id, n, slow, disconnect)
			mu.Lock()
			records = append(records, r)
			mu.Unlock()
		}(i+1, tokens, slow, disconnect)
	}
	if ctx.Err() == nil {
		_ = wait(ctx, time.Until(start.Add(c.Duration)))
	}
	wg.Wait()
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	elapsed := time.Since(start).Seconds()
	summary := Summarize(c.RPS, elapsed, skipped, records)
	return Report{summary, records}, ctx.Err()
}

func generate(parent context.Context, client *http.Client, c Config, epoch time.Time, id, n int, slow, disconnect bool) (r Record) {
	start := time.Now()
	r = Record{ID: id, MaxTokens: n, Slow: slow, Disconnect: disconnect, StartMS: float64(start.Sub(epoch)) / float64(time.Millisecond), Status: "error"}
	defer func() { r.CompletionMS = float64(time.Since(start)) / float64(time.Millisecond) }()
	ctx, cancel := context.WithTimeout(parent, c.Timeout)
	defer cancel()
	body, _ := json.Marshal(struct {
		Prompt    string `json:"prompt"`
		MaxTokens int    `json:"max_tokens"`
	}{c.Prompt, n})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/generate", bytes.NewReader(body))
	if err != nil {
		r.Error = err.Error()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		r.Error = err.Error()
		return
	}
	defer resp.Body.Close()
	r.RequestID = resp.Header.Get("X-Request-ID")
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		r.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
		return
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		r.Error = "expected text/event-stream"
		return
	}
	scan := bufio.NewScanner(resp.Body)
	var event, data string
	var last time.Time
	for scan.Scan() {
		line := scan.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		case line == "":
			switch event {
			case "token":
				var token model.Token
				if err := json.Unmarshal([]byte(data), &token); err != nil {
					r.Error = err.Error()
					return
				}
				if token.Index != r.Tokens+1 {
					r.Error = "out-of-order token"
					return
				}
				now := time.Now()
				if r.Tokens == 0 {
					r.TTFTMS = float64(now.Sub(start)) / float64(time.Millisecond)
				} else {
					r.GapsMS = append(r.GapsMS, float64(now.Sub(last))/float64(time.Millisecond))
				}
				last = now
				r.Tokens++
				if disconnect && r.Tokens >= c.DisconnectAfter {
					cancel()
					r.Status = "disconnected"
					return
				}
				if slow {
					if err := wait(ctx, c.SlowDelay); err != nil {
						r.Error = err.Error()
						return
					}
				}
			case "done":
				var result scheduler.Result
				if err := json.Unmarshal([]byte(data), &result); err != nil {
					r.Error = err.Error()
					return
				}
				if result.Reason == "completed" && r.Tokens == n && result.Tokens == r.Tokens {
					r.Status = "completed"
				} else {
					r.Error = fmt.Sprintf("done: %s (%d/%d tokens)", result.Reason, r.Tokens, n)
				}
				return
			}
			event = ""
			data = ""
		}
	}
	if err := scan.Err(); err != nil {
		r.Error = err.Error()
	} else {
		r.Error = "stream ended without done event"
	}
	return
}

func Summarize(rps, elapsed float64, skipped int, records []Record) Summary {
	s := Summary{RPS: rps, Requests: len(records), Skipped: skipped, ElapsedSeconds: elapsed}
	var ttft, completion, gaps []float64
	for _, r := range records {
		s.Tokens += r.Tokens
		if r.Tokens > 0 {
			ttft = append(ttft, r.TTFTMS)
		}
		gaps = append(gaps, r.GapsMS...)
		switch r.Status {
		case "completed":
			s.Completed++
			completion = append(completion, r.CompletionMS)
		case "disconnected":
			s.Disconnected++
		default:
			s.Errors++
		}
	}
	if elapsed > 0 {
		s.TokensPerSecond = float64(s.Tokens) / elapsed
	}
	s.TTFT = metrics.Quantiles(ttft)
	s.Completion = metrics.Quantiles(completion)
	s.Gaps = metrics.Quantiles(gaps)
	return s
}
func Print(w io.Writer, s Summary) {
	fmt.Fprintf(w, "target %.2f req/s | launched %d | completed %d | disconnected %d | errors %d | skipped %d\n", s.RPS, s.Requests, s.Completed, s.Disconnected, s.Errors, s.Skipped)
	fmt.Fprintf(w, "received %d tokens in %.2fs = %.2f tokens/s (includes drain)\n", s.Tokens, s.ElapsedSeconds, s.TokensPerSecond)
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "latency (ms)\tsamples\tp50\tp95\tp99")
	for _, row := range []struct {
		name string
		p    metrics.Percentiles
	}{{"first token", s.TTFT}, {"token gap", s.Gaps}, {"completion", s.Completion}} {
		fmt.Fprintf(tw, "%s\t%d\t%.2f\t%.2f\t%.2f\n", row.name, row.p.Samples, row.p.P50, row.p.P95, row.p.P99)
	}
	_ = tw.Flush()
}
func WriteCSV(path string, records []Record) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"id", "request_id", "max_tokens", "tokens", "slow", "disconnect", "start_ms", "ttft_ms", "completion_ms", "gap_p50_ms", "gap_p99_ms", "gaps_ms", "status", "error"})
	num := func(x float64) string { return strconv.FormatFloat(x, 'f', 3, 64) }
	for _, r := range records {
		gaps, _ := json.Marshal(r.GapsMS)
		p := metrics.Quantiles(r.GapsMS)
		_ = w.Write([]string{strconv.Itoa(r.ID), r.RequestID, strconv.Itoa(r.MaxTokens), strconv.Itoa(r.Tokens), strconv.FormatBool(r.Slow), strconv.FormatBool(r.Disconnect), num(r.StartMS), num(r.TTFTMS), num(r.CompletionMS), num(p.P50), num(p.P99), string(gaps), r.Status, r.Error})
	}
	w.Flush()
	writeErr := w.Error()
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}
