package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"continuous-batching-lab/internal/loadgen"
)

func main() {
	c := loadgen.DefaultConfig()
	flag.StringVar(&c.URL, "url", c.URL, "server base URL")
	flag.Float64Var(&c.RPS, "rps", c.RPS, "target requests per second")
	flag.DurationVar(&c.Duration, "duration", c.Duration, "arrival window")
	flag.DurationVar(&c.Timeout, "timeout", c.Timeout, "per-request timeout including queue and stream")
	flag.StringVar(&c.Prompt, "prompt", c.Prompt, "prompt for each request")
	flag.IntVar(&c.ShortTokens, "short-tokens", c.ShortTokens, "short request output length")
	flag.IntVar(&c.LongTokens, "long-tokens", c.LongTokens, "long request output length")
	flag.Float64Var(&c.LongFraction, "long-fraction", c.LongFraction, "fraction of long requests")
	flag.Float64Var(&c.SlowFraction, "slow-fraction", c.SlowFraction, "fraction of slow readers")
	flag.DurationVar(&c.SlowDelay, "slow-delay", c.SlowDelay, "delay after each token for slow readers")
	flag.Float64Var(&c.DisconnectFraction, "disconnect-fraction", c.DisconnectFraction, "fraction disconnecting early")
	flag.IntVar(&c.DisconnectAfter, "disconnect-after", c.DisconnectAfter, "disconnect after this many tokens")
	flag.IntVar(&c.MaxInflight, "max-inflight", c.MaxInflight, "client limit; excess arrivals counted as skipped")
	flag.Int64Var(&c.Seed, "seed", c.Seed, "reproducible workload mix seed")
	csv := flag.String("csv", "loadgen.csv", "per-request CSV destination")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := loadgen.Run(ctx, c)
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
	loadgen.Print(os.Stdout, report.Summary)
	if err := loadgen.WriteCSV(*csv, report.Records); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s", *csv)
	if report.Summary.Errors > 0 {
		os.Exit(1)
	}
}
