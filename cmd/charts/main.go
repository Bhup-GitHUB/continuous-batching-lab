package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image/color"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"continuous-batching-lab/internal/clock"
	"continuous-batching-lab/internal/loadgen"
	"continuous-batching-lab/internal/model"
	"continuous-batching-lab/internal/scheduler"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

type point struct {
	Policy  scheduler.Policy
	Summary loadgen.Summary
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	ratesFlag := flag.String("rates", "2,5,8,12", "comma-separated arrival rates")
	duration := flag.Duration("duration", 5*time.Second, "arrival window per policy and rate")
	out := flag.String("out", "out", "output directory")
	maxActive := flag.Int("max-active", 8, "decode slots in rate sweeps")
	stepMS := flag.Float64("step-base-ms", 20, "decode batch cost in milliseconds")
	seed := flag.Int64("seed", 1, "same workload seed for both policies")
	flag.Parse()
	if math.IsNaN(*stepMS) || math.IsInf(*stepMS, 0) || *stepMS <= 0 || *stepMS > 60000 {
		return errors.New("step-base-ms must be in (0, 60000]")
	}
	rates, err := parseRates(*ratesFlag)
	if err != nil {
		return err
	}
	if *duration <= 0 || *maxActive < 1 {
		return errors.New("duration and max-active must be positive")
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	costs := model.DefaultCosts()
	costs.StepBase = time.Duration(*stepMS * float64(time.Millisecond))
	var points []point
	for _, policy := range []scheduler.Policy{scheduler.Continuous, scheduler.Fixed} {
		for i, rate := range rates {
			c := scheduler.DefaultConfig()
			c.Policy = policy
			c.MaxActive = *maxActive
			lc := loadgen.DefaultConfig()
			lc.RPS = rate
			lc.Duration = *duration
			lc.Seed = *seed
			report, _, err := benchmark(ctx, c, costs, lc)
			if err != nil {
				return err
			}
			path := filepath.Join(*out, fmt.Sprintf("%s_rate_%d.csv", policy, i+1))
			if err := loadgen.WriteCSV(path, report.Records); err != nil {
				return err
			}
			fmt.Printf("\n%s at %.2f req/s\n", policy, rate)
			loadgen.Print(os.Stdout, report.Summary)
			if report.Summary.Errors > 0 || report.Summary.Skipped > 0 {
				return fmt.Errorf("%s at %.2f rps has errors or skipped arrivals; inspect %s", policy, rate, path)
			}
			points = append(points, point{policy, report.Summary})
		}
	}
	if err := summaryCSV(filepath.Join(*out, "summary.csv"), points); err != nil {
		return err
	}
	if err := ratePlot(filepath.Join(*out, "ttft_vs_rps.png"), points, true); err != nil {
		return err
	}
	if err := ratePlot(filepath.Join(*out, "throughput_vs_rps.png"), points, false); err != nil {
		return err
	}
	traces := make(map[scheduler.Policy][]scheduler.Trace)
	for _, policy := range []scheduler.Policy{scheduler.Continuous, scheduler.Fixed} {
		c := scheduler.DefaultConfig()
		c.Policy = policy
		c.MaxActive = 2
		c.TraceLimit = 100
		lc := loadgen.DefaultConfig()
		lc.RPS = 20
		lc.Duration = 400 * time.Millisecond
		lc.ShortTokens = 8
		lc.LongTokens = 32
		lc.LongFraction = .4
		lc.Seed = *seed
		report, view, err := benchmark(ctx, c, costs, lc)
		if err != nil {
			return err
		}
		if err := loadgen.WriteCSV(filepath.Join(*out, string(policy)+"_timeline.csv"), report.Records); err != nil {
			return err
		}
		if report.Summary.Errors > 0 || view.TraceDropped > 0 || len(view.Trace) != report.Summary.Completed {
			return errors.New("incomplete timeline run")
		}
		traces[policy] = view.Trace
		file, err := os.Create(filepath.Join(*out, string(policy)+"_trace.json"))
		if err != nil {
			return err
		}
		encodeErr := json.NewEncoder(file).Encode(view)
		closeErr := file.Close()
		if err := errors.Join(encodeErr, closeErr); err != nil {
			return err
		}
	}
	if err := timeline(filepath.Join(*out, "timeline.png"), traces); err != nil {
		return err
	}
	fmt.Printf("\nCharts and raw measurements written to %s\n", *out)
	return nil
}
func parseRates(input string) ([]float64, error) {
	var rates []float64
	for _, part := range strings.Split(input, ",") {
		r, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || math.IsNaN(r) || math.IsInf(r, 0) || r <= 0 || r > 1e6 {
			return nil, fmt.Errorf("invalid rate %q", part)
		}
		rates = append(rates, r)
	}
	for i := 1; i < len(rates); i++ {
		if rates[i] <= rates[i-1] {
			return nil, errors.New("rates must be strictly increasing")
		}
	}
	return rates, nil
}
func benchmark(ctx context.Context, c scheduler.Config, costs model.Costs, lc loadgen.Config) (loadgen.Report, scheduler.View, error) {
	clk := clock.Real{}
	s, err := scheduler.New(c, clk, &model.Fake{Clock: clk, Costs: costs})
	if err != nil {
		return loadgen.Report{}, scheduler.View{}, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return loadgen.Report{}, scheduler.View{}, err
	}
	serverCtx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() { defer close(finished); s.Run(serverCtx) }()
	server := &http.Server{Handler: scheduler.Handler(s), ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	defer func() { cancel(); _ = server.Close(); <-finished; <-serverErr }()
	lc.URL = "http://" + listener.Addr().String()
	report, err := loadgen.Run(ctx, lc)
	if err != nil {
		return report, scheduler.View{}, err
	}
	requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
	defer requestCancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, lc.URL+"/trace", nil)
	if err != nil {
		return report, scheduler.View{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return report, scheduler.View{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return report, scheduler.View{}, fmt.Errorf("trace HTTP status %s", resp.Status)
	}
	var view scheduler.View
	err = json.NewDecoder(resp.Body).Decode(&view)
	return report, view, err
}

var policyColors = map[scheduler.Policy]color.RGBA{scheduler.Continuous: {R: 0, G: 125, B: 145, A: 255}, scheduler.Fixed: {R: 210, G: 90, B: 25, A: 255}}

func ratePlot(path string, points []point, ttft bool) error {
	p := plot.New()
	p.X.Label.Text = "Target arrival rate (requests / second)"
	p.Add(plotter.NewGrid())
	p.Legend.Top = true
	p.Legend.Left = true
	if ttft {
		p.Title.Text = "Time to first token by scheduling policy"
		p.Y.Label.Text = "Client TTFT (milliseconds)"
	} else {
		p.Title.Text = "Delivered token throughput by scheduling policy"
		p.Y.Label.Text = "Tokens / second (arrival window + drain)"
	}
	for _, policy := range []scheduler.Policy{scheduler.Continuous, scheduler.Fixed} {
		percentiles := []string{"p50"}
		if ttft {
			percentiles = append(percentiles, "p99")
		}
		for _, percentile := range percentiles {
			var xy plotter.XYs
			for _, point := range points {
				if point.Policy != policy {
					continue
				}
				y := point.Summary.TokensPerSecond
				if ttft {
					y = point.Summary.TTFT.P50
					if percentile == "p99" {
						y = point.Summary.TTFT.P99
					}
				}
				xy = append(xy, plotter.XY{X: point.Summary.RPS, Y: y})
			}
			line, dots, err := plotter.NewLinePoints(xy)
			if err != nil {
				return err
			}
			line.Color = policyColors[policy]
			line.Width = vg.Points(2)
			dots.Color = policyColors[policy]
			dots.Radius = vg.Points(3)
			dots.Shape = draw.CircleGlyph{}
			label := string(policy)
			if ttft {
				label += " " + percentile
			}
			if percentile == "p99" {
				line.Dashes = []vg.Length{vg.Points(6), vg.Points(4)}
				dots.Shape = draw.SquareGlyph{}
			}
			p.Add(line, dots)
			p.Legend.Add(label, line, dots)
		}
	}
	p.Y.Min = 0
	return p.Save(9*vg.Inch, 5*vg.Inch, path)
}
func timeline(path string, traces map[scheduler.Policy][]scheduler.Trace) error {
	p := plot.New()
	p.Title.Text = "Slot occupancy: same 8-request workload, 2 slots per policy"
	p.X.Label.Text = "Milliseconds since server start (includes idle time and prefill)"
	p.Y.Label.Text = "Policy / slot"
	p.Add(plotter.NewGrid())
	p.Y.Tick.Marker = plot.ConstantTicks{{Value: 0, Label: "fixed / 0"}, {Value: 1, Label: "fixed / 1"}, {Value: 3, Label: "continuous / 0"}, {Value: 4, Label: "continuous / 1"}}
	palette := []color.RGBA{{65, 125, 185, 255}, {240, 175, 45, 255}, {50, 160, 125, 255}, {180, 105, 155, 255}, {135, 155, 55, 255}, {210, 110, 75, 255}, {100, 145, 155, 255}, {145, 115, 180, 255}}
	var labels plotter.XYLabels
	end := 0.0
	for _, policy := range []scheduler.Policy{scheduler.Continuous, scheduler.Fixed} {
		offset := 0.0
		if policy == scheduler.Continuous {
			offset = 3
		}
		for _, tr := range traces[policy] {
			y := float64(tr.Slot) + offset
			x0, x1 := tr.StartMS, tr.EndMS
			end = math.Max(end, x1)
			box, err := plotter.NewPolygon(plotter.XYs{{X: x0, Y: y - .32}, {X: x1, Y: y - .32}, {X: x1, Y: y + .32}, {X: x0, Y: y + .32}})
			if err != nil {
				return err
			}
			id, _ := strconv.Atoi(tr.ID)
			box.Color = palette[(max(1, id)-1)%len(palette)]
			box.LineStyle.Color = color.White
			box.LineStyle.Width = vg.Points(1)
			p.Add(box)
			labels.XYs = append(labels.XYs, plotter.XY{X: (x0 + x1) / 2, Y: y})
			labels.Labels = append(labels.Labels, "R"+tr.ID)
		}
	}
	l, err := plotter.NewLabels(labels)
	if err != nil {
		return err
	}
	for i := range l.TextStyle {
		l.TextStyle[i].XAlign = draw.XCenter
		l.TextStyle[i].YAlign = draw.YCenter
	}
	p.Add(l)
	p.Y.Min = -.7
	p.Y.Max = 4.7
	p.X.Min = 0
	p.X.Max = end * 1.03
	return p.Save(11*vg.Inch, 4*vg.Inch, path)
}
func summaryCSV(path string, points []point) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"policy", "target_rps", "requests", "completed", "errors", "tokens", "elapsed_s", "tokens_per_s", "ttft_p50_ms", "ttft_p99_ms"})
	for _, p := range points {
		s := p.Summary
		_ = w.Write([]string{string(p.Policy), fmt.Sprint(s.RPS), fmt.Sprint(s.Requests), fmt.Sprint(s.Completed), fmt.Sprint(s.Errors), fmt.Sprint(s.Tokens), fmt.Sprint(s.ElapsedSeconds), fmt.Sprint(s.TokensPerSecond), fmt.Sprint(s.TTFT.P50), fmt.Sprint(s.TTFT.P99)})
	}
	w.Flush()
	writeErr := w.Error()
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}
