package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"continuous-batching-lab/internal/clock"
	"continuous-batching-lab/internal/model"
	"continuous-batching-lab/internal/scheduler"
)

func main() {
	c := scheduler.DefaultConfig()
	policy := flag.String("policy", string(c.Policy), "continuous or fixed")
	flag.IntVar(&c.MaxActive, "max-active", c.MaxActive, "maximum occupied decode slots")
	flag.IntVar(&c.KVBudget, "kv-budget", c.KVBudget, "prompt + output token reservations")
	flag.IntVar(&c.PrefillBudget, "prefill-budget", c.PrefillBudget, "whole-prompt tokens per iteration")
	flag.IntVar(&c.SubmitBuffer, "submit-buffer", c.SubmitBuffer, "submission queue capacity")
	flag.IntVar(&c.OutputBuffer, "output-buffer", c.OutputBuffer, "tokens buffered per request")
	flag.IntVar(&c.TraceLimit, "trace-limit", c.TraceLimit, "maximum retired slot intervals to retain; 0 disables")
	flag.DurationVar(&c.StallTimeout, "stall-timeout", c.StallTimeout, "cancel requests stalled this long")
	stepBase := flag.Float64("step-base-ms", 20, "decode cost per batch in milliseconds")
	stepPerActive := flag.Float64("step-per-active-ms", .3, "additional decode cost per runnable request")
	prefillBase := flag.Float64("prefill-base-ms", 2, "prefill cost per prompt")
	prefillPerToken := flag.Float64("prefill-per-token-ms", .1, "prefill cost per prompt token")
	port := flag.Int("port", 8080, "HTTP port")
	flag.Parse()
	c.Policy = scheduler.Policy(*policy)
	ms := func(v float64) time.Duration {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 60000 {
			log.Fatal("model costs must be between 0 and 60000 ms")
		}
		return time.Duration(v * float64(time.Millisecond))
	}
	clk := clock.Real{}
	m := &model.Fake{Clock: clk, Costs: model.Costs{PrefillBase: ms(*prefillBase), PrefillPerToken: ms(*prefillPerToken), StepBase: ms(*stepBase), StepPerActive: ms(*stepPerActive)}}
	s, err := scheduler.New(c, clk, m)
	if err != nil {
		log.Fatal(err)
	}
	if *port < 1 || *port > 65535 {
		log.Fatal("port must be between 1 and 65535")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan struct{})
	go func() { defer close(finished); s.Run(ctx) }()
	server := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: scheduler.Handler(s), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	log.Printf("policy=%s max_active=%d kv_budget=%d listening on %s", c.Policy, c.MaxActive, c.KVBudget, server.Addr)
	select {
	case <-ctx.Done():
	case err = <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
		}
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
	}
	<-finished
}
