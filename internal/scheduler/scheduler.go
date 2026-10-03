package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"continuous-batching-lab/internal/clock"
	"continuous-batching-lab/internal/metrics"
	"continuous-batching-lab/internal/model"
)

type Policy string

const (
	Continuous Policy = "continuous"
	Fixed      Policy = "fixed"
)

type Config struct {
	Policy                                                         Policy
	MaxActive, KVBudget, PrefillBudget, SubmitBuffer, OutputBuffer int
	StallTimeout, PollInterval                                     time.Duration
	TraceLimit                                                     int
}

func DefaultConfig() Config {
	return Config{
		Policy:        Continuous,
		MaxActive:     32,
		KVBudget:      20000,
		PrefillBudget: 512,
		SubmitBuffer:  1024,
		OutputBuffer:  16,
		StallTimeout:  5 * time.Second,
		PollInterval:  time.Millisecond,
	}
}
func (c Config) Validate() error {
	if c.Policy != Continuous && c.Policy != Fixed {
		return errors.New("policy must be continuous or fixed")
	}
	if c.MaxActive < 1 || c.KVBudget < 1 || c.PrefillBudget < 1 || c.SubmitBuffer < 1 || c.OutputBuffer < 1 || c.StallTimeout <= 0 || c.PollInterval <= 0 || c.TraceLimit < 0 {
		return errors.New("budgets, buffers, slots and timeouts must be positive (trace limit may be zero)")
	}
	return nil
}

type Result struct {
	Reason string `json:"reason"`
	Tokens int    `json:"tokens"`
}
type Request struct {
	ID, Prompt              string
	MaxTokens, PromptTokens int
	submitted               time.Time
	Ctx                     context.Context
	Cancel                  context.CancelFunc
	Out                     chan model.Token
	Result                  chan Result
}

func NewRequest(ctx context.Context, id, prompt string, maxTokens, outputBuffer int) *Request {
	ctx, cancel := context.WithCancel(ctx)
	return &Request{ID: id, Prompt: prompt, MaxTokens: maxTokens, PromptTokens: max(1, len(strings.Fields(prompt))), Ctx: ctx, Cancel: cancel, Out: make(chan model.Token, outputBuffer), Result: make(chan Result, 1)}
}

type state struct {
	req                        *Request
	submitted, admitted        time.Time
	generated, delivered, slot int
	reservation                int
	first                      bool
	paused                     bool
	pausedAt                   time.Time
	pending                    *model.Token
}
type Trace struct {
	ID      string  `json:"id"`
	Slot    int     `json:"slot"`
	StartMS float64 `json:"start_ms"`
	EndMS   float64 `json:"end_ms"`
	Tokens  int     `json:"tokens"`
	Reason  string  `json:"reason"`
}
type View struct {
	Metrics      metrics.Snapshot `json:"metrics"`
	Trace        []Trace          `json:"trace"`
	TraceDropped uint64           `json:"trace_dropped"`
}
type query struct {
	trace bool
	reply chan View
}

type Scheduler struct {
	config                 Config
	clock                  clock.Clock
	model                  model.Model
	submit                 chan *Request
	queries                chan query
	done                   chan struct{}
	waiting, ready, active []*state
	slots                  []bool
	kvUsed                 int
	totals                 metrics.Totals
	ttft, completion       metrics.Samples
	epoch                  time.Time
	trace                  []Trace
	traceDropped           uint64
}

func New(c Config, clk clock.Clock, m model.Model) (*Scheduler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &Scheduler{config: c, clock: clk, model: m, submit: make(chan *Request, c.SubmitBuffer), queries: make(chan query), done: make(chan struct{}), slots: make([]bool, c.MaxActive), epoch: clk.Now()}, nil
}
func (s *Scheduler) ValidateRequest(r *Request) error {
	if r.MaxTokens < 1 {
		return errors.New("max_tokens must be positive")
	}
	if r.PromptTokens > s.config.PrefillBudget {
		return fmt.Errorf("prompt exceeds whole-prompt prefill budget (%d)", s.config.PrefillBudget)
	}
	if r.PromptTokens > s.config.KVBudget || r.MaxTokens > s.config.KVBudget-r.PromptTokens {
		return errors.New("prompt_tokens + max_tokens exceeds KV budget")
	}
	return nil
}
func (s *Scheduler) Submit(ctx context.Context, r *Request) error {
	if err := s.ValidateRequest(r); err != nil {
		return err
	}
	select {
	case <-s.done:
		return errors.New("scheduler stopped")
	default:
	}
	r.submitted = s.clock.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.Ctx.Done():
		return r.Ctx.Err()
	case <-s.done:
		return errors.New("scheduler stopped")
	case s.submit <- r:
		select {
		case <-s.done:
			return errors.New("scheduler stopped")
		default:
			return nil
		}
	}
}
func (s *Scheduler) Snapshot(ctx context.Context, trace bool) (View, error) {
	q := query{trace: trace, reply: make(chan View, 1)}
	select {
	case s.queries <- q:
	case <-ctx.Done():
		return View{}, ctx.Err()
	case <-s.done:
		return View{}, errors.New("scheduler stopped")
	}
	select {
	case v := <-q.reply:
		return v, nil
	case <-ctx.Done():
		return View{}, ctx.Err()
	case <-s.done:
		return View{}, errors.New("scheduler stopped")
	}
}

func (s *Scheduler) accept(r *Request) {
	s.totals.Submitted++
	s.waiting = append(s.waiting, &state{req: r, submitted: r.submitted, slot: -1})
}
func (s *Scheduler) answer(q query) {
	paused := 0
	for _, r := range s.active {
		if r.paused {
			paused++
		}
	}
	v := View{Metrics: metrics.Snapshot{Policy: string(s.config.Policy), Waiting: len(s.waiting), Ready: len(s.ready), Active: len(s.active), Paused: paused, KVUsed: s.kvUsed, KVLimit: s.config.KVBudget, Totals: s.totals, TTFT: s.ttft.Percentiles(), Completion: s.completion.Percentiles()}, TraceDropped: s.traceDropped}
	if q.trace {
		v.Trace = append([]Trace{}, s.trace...)
	}
	q.reply <- v
}

func (s *Scheduler) Run(ctx context.Context) {
	defer func() { close(s.done); s.shutdown() }()
	for ctx.Err() == nil {
		if err := s.iteration(ctx); err != nil {
			return
		}
		select {
		case q := <-s.queries:
			s.answer(q)
		default:
		}
		if len(s.waiting)+len(s.ready)+len(s.active) == 0 {
			select {
			case r := <-s.submit:
				s.accept(r)
			case q := <-s.queries:
				s.answer(q)
			case <-ctx.Done():
				return
			}
		} else if s.allPaused() {
			if err := s.clock.Sleep(ctx, s.config.PollInterval); err != nil {
				return
			}
		}
	}
}

func (s *Scheduler) iteration(ctx context.Context) error {
	for n := len(s.submit); n > 0; n-- {
		s.accept(<-s.submit)
	}
	s.waiting = s.removeCancelled(s.waiting)
	s.ready = s.removeCancelled(s.ready)
	s.active = s.removeCancelled(s.active)
	budget := s.config.PrefillBudget
	for len(s.waiting) > 0 && s.waiting[0].req.PromptTokens <= budget {
		r := s.waiting[0]
		if err := s.model.Prefill(ctx, r.req.PromptTokens); err != nil {
			return err
		}
		budget -= r.req.PromptTokens
		s.waiting = s.waiting[1:]
		if r.req.Ctx.Err() != nil {
			s.retire(r, "cancelled")
		} else {
			s.ready = append(s.ready, r)
		}
	}
	if s.config.Policy == Continuous || len(s.active) == 0 {
		for len(s.ready) > 0 && len(s.active) < s.config.MaxActive {
			r := s.ready[0]
			reservation := r.req.PromptTokens + r.req.MaxTokens
			if s.kvUsed+reservation > s.config.KVBudget {
				break
			}
			s.ready = s.ready[1:]
			if r.req.Ctx.Err() != nil {
				s.retire(r, "cancelled")
				continue
			}
			for i, used := range s.slots {
				if !used {
					r.slot = i
					s.slots[i] = true
					break
				}
			}
			r.reservation = reservation
			r.admitted = s.clock.Now()
			s.kvUsed += reservation
			s.active = append(s.active, r)
		}
	}
	for _, r := range s.active {
		if !r.paused {
			continue
		}
		if s.clock.Now().Sub(r.pausedAt) >= s.config.StallTimeout {
			continue
		}
		if r.pending != nil && s.deliver(r, *r.pending) {
			r.pending = nil
		}
		if r.pending == nil && len(r.req.Out) < cap(r.req.Out) {
			r.paused = false
		}
	}
	var runnable []*state
	var batch []model.Sequence
	for _, r := range s.active {
		if !r.paused && r.generated < r.req.MaxTokens && r.req.Ctx.Err() == nil {
			runnable = append(runnable, r)
			batch = append(batch, model.Sequence{ID: r.req.ID, Next: r.generated + 1})
		}
	}
	if len(batch) > 0 {
		tokens, err := s.model.DecodeStep(ctx, batch)
		if err != nil {
			return err
		}
		s.totals.DecodeSteps++
		for i, r := range runnable {
			r.generated++
			s.totals.GeneratedTokens++
			if r.req.Ctx.Err() != nil {
				continue
			}
			if !s.deliver(r, tokens[i]) {
				t := tokens[i]
				r.pending = &t
				r.paused = true
				r.pausedAt = s.clock.Now()
			}
		}
	}
	kept := s.active[:0]
	for _, r := range s.active {
		switch {
		case r.req.Ctx.Err() != nil:
			s.retire(r, "cancelled")
		case r.paused && s.clock.Now().Sub(r.pausedAt) >= s.config.StallTimeout:
			s.retire(r, "stalled")
		case r.delivered == r.req.MaxTokens:
			s.retire(r, "completed")
		default:
			kept = append(kept, r)
		}
	}
	s.active = kept
	return nil
}
func (s *Scheduler) deliver(r *state, t model.Token) bool {
	select {
	case r.req.Out <- t:
		r.delivered++
		s.totals.DeliveredTokens++
		if !r.first {
			r.first = true
			s.ttft.Observe(s.clock.Now().Sub(r.submitted))
		}
		return true
	default:
		return false
	}
}
func (s *Scheduler) retire(r *state, reason string) {
	if r.slot >= 0 {
		s.slots[r.slot] = false
		s.kvUsed -= r.reservation
		if s.config.TraceLimit > 0 {
			if len(s.trace) < s.config.TraceLimit {
				s.trace = append(s.trace, Trace{r.req.ID, r.slot, float64(r.admitted.Sub(s.epoch)) / float64(time.Millisecond), float64(s.clock.Now().Sub(s.epoch)) / float64(time.Millisecond), r.delivered, reason})
			} else {
				s.traceDropped++
			}
		}
	}
	switch reason {
	case "completed":
		s.totals.Completed++
		s.completion.Observe(s.clock.Now().Sub(r.submitted))
	case "stalled":
		s.totals.Stalled++
		s.totals.Cancelled++
	default:
		s.totals.Cancelled++
	}
	r.req.Result <- Result{reason, r.delivered}
	close(r.req.Out)
	close(r.req.Result)
	r.req.Cancel()
}
func (s *Scheduler) removeCancelled(set []*state) []*state {
	kept := set[:0]
	for _, r := range set {
		if r.req.Ctx.Err() != nil {
			s.retire(r, "cancelled")
		} else {
			kept = append(kept, r)
		}
	}
	return kept
}
func (s *Scheduler) allPaused() bool {
	if len(s.waiting) > 0 || len(s.active) == 0 {
		return false
	}
	for _, r := range s.active {
		if !r.paused {
			return false
		}
	}
	return true
}
func (s *Scheduler) shutdown() {
	for _, set := range [][]*state{s.waiting, s.ready, s.active} {
		for _, r := range set {
			s.retire(r, "shutdown")
		}
	}
	for {
		select {
		case r := <-s.submit:
			s.accept(r)
			s.retire(s.waiting[len(s.waiting)-1], "shutdown")
		default:
			return
		}
	}
}
