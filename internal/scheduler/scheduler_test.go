package scheduler

import (
	"context"
	"reflect"
	"testing"
	"time"

	"continuous-batching-lab/internal/clock"
	"continuous-batching-lab/internal/model"
)

func testScheduler(t *testing.T, policy Policy, change func(*Config)) (*Scheduler, *clock.Fake) {
	t.Helper()
	c := DefaultConfig()
	c.Policy = policy
	c.MaxActive = 2
	c.TraceLimit = 100
	c.OutputBuffer = 32
	if change != nil {
		change(&c)
	}
	clk := clock.NewFake()
	s, err := New(c, clk, &model.Fake{Clock: clk, Costs: model.Costs{StepBase: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	return s, clk
}
func submit(t *testing.T, s *Scheduler, id string, n int) *Request {
	t.Helper()
	r := NewRequest(context.Background(), id, "prompt", n, s.config.OutputBuffer)
	t.Cleanup(r.Cancel)
	if err := s.Submit(r.Ctx, r); err != nil {
		t.Fatal(err)
	}
	return r
}
func step(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.iteration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.kvUsed < 0 || s.kvUsed > s.config.KVBudget {
		t.Fatalf("KV used = %d", s.kvUsed)
	}
}
func finish(t *testing.T, s *Scheduler) {
	t.Helper()
	for n := 0; len(s.submit)+len(s.waiting)+len(s.ready)+len(s.active) > 0; n++ {
		if n > 100 {
			t.Fatal("scheduler did not finish")
		}
		step(t, s)
	}
}
func TestABC(t *testing.T) {
	for _, tc := range []struct {
		policy      Policy
		end, startC int
	}{{Fixed, 8, 7}, {Continuous, 6, 3}} {
		t.Run(string(tc.policy), func(t *testing.T) {
			s, clk := testScheduler(t, tc.policy, nil)
			submit(t, s, "A", 2)
			submit(t, s, "B", 6)
			submit(t, s, "C", 2)
			finish(t, s)
			if got := int(clk.Now().Sub(s.epoch) / time.Second); got != tc.end {
				t.Fatalf("finished at %d, want %d", got, tc.end)
			}
			for _, tr := range s.trace {
				if tr.ID == "C" && int(tr.StartMS/1000)+1 != tc.startC {
					t.Fatalf("C first decode step = %g, want %d", tr.StartMS/1000+1, tc.startC)
				}
			}
			if len(s.trace) != 3 || s.kvUsed != 0 {
				t.Fatalf("trace=%v kv=%d", s.trace, s.kvUsed)
			}
		})
	}
}
func TestTokensInOrderAndFinalPendingToken(t *testing.T) {
	s, _ := testScheduler(t, Continuous, func(c *Config) { c.OutputBuffer = 1 })
	r := submit(t, s, "ordered", 5)
	step(t, s)
	step(t, s)
	if !s.active[0].paused || s.active[0].pending.Index != 2 {
		t.Fatal("expected retained token 2")
	}
	for want := 1; want <= 5; want++ {
		got := <-r.Out
		if got.Index != want || got.Text != "ordered:"+string(rune('0'+want)) {
			t.Fatalf("got %v, want index %d", got, want)
		}
		if want < 5 {
			step(t, s)
		}
	}
	finish(t, s)
	if _, ok := <-r.Out; ok {
		t.Fatal("output not closed")
	}
	if result := <-r.Result; result.Reason != "completed" || result.Tokens != 5 {
		t.Fatalf("result=%v", result)
	}
}
func TestCancellationFreesSlotAndKV(t *testing.T) {
	s, _ := testScheduler(t, Continuous, func(c *Config) { c.MaxActive = 1; c.KVBudget = 11 })
	a := submit(t, s, "A", 10)
	b := submit(t, s, "B", 2)
	step(t, s)
	if s.kvUsed != 11 || len(s.ready) != 1 {
		t.Fatal("expected full reservation and ready request")
	}
	a.Cancel()
	step(t, s)
	if len(s.active) != 1 || s.active[0].req != b || s.kvUsed != 3 {
		t.Fatal("cancelled request did not release its slot and reservation")
	}
	if result := <-a.Result; result.Reason != "cancelled" {
		t.Fatalf("result=%v", result)
	}
	finish(t, s)
	if s.kvUsed != 0 || s.totals.Cancelled != 1 {
		t.Fatal("reservation leaked")
	}
}
func TestCancelledWaitingAndReady(t *testing.T) {
	s, _ := testScheduler(t, Continuous, func(c *Config) { c.PrefillBudget = 1; c.MaxActive = 1 })
	a := submit(t, s, "A", 10)
	b := submit(t, s, "B", 2)
	c := submit(t, s, "C", 2)
	step(t, s)
	step(t, s)
	if len(s.ready) != 1 || len(s.waiting) != 1 {
		t.Fatal("expected ready and waiting requests")
	}
	b.Cancel()
	c.Cancel()
	step(t, s)
	if len(s.ready)+len(s.waiting) != 0 || s.totals.Cancelled != 2 {
		t.Fatal("cancelled queues not pruned")
	}
	a.Cancel()
	step(t, s)
}
func TestKVBudgetNeverExceeded(t *testing.T) {
	for _, policy := range []Policy{Continuous, Fixed} {
		s, _ := testScheduler(t, policy, func(c *Config) { c.MaxActive = 8; c.KVBudget = 12 })
		for i, n := range []int{5, 5, 2, 4, 3, 6, 1} {
			submit(t, s, string(rune('A'+i)), n)
		}
		finish(t, s)
		if s.totals.Completed != 7 || s.kvUsed != 0 {
			t.Fatalf("totals=%v kv=%d", s.totals, s.kvUsed)
		}
		bad := NewRequest(context.Background(), "oversized", "prompt", 12, 1)
		if err := s.Submit(bad.Ctx, bad); err == nil {
			t.Fatal("accepted request larger than budget")
		}
		bad.Cancel()
	}
}
func TestSlowClientOnlyPausesItself(t *testing.T) {
	s, _ := testScheduler(t, Continuous, func(c *Config) { c.OutputBuffer = 1; c.StallTimeout = 3 * time.Second })
	slow := submit(t, s, "slow", 10)
	fast := submit(t, s, "fast", 6)
	for i := 0; i < 6; i++ {
		step(t, s)
		if token := <-fast.Out; token.Index != i+1 {
			t.Fatal(token)
		}
	}
	if result := <-fast.Result; result.Reason != "completed" {
		t.Fatal(result)
	}
	if result := <-slow.Result; result.Reason != "stalled" || result.Tokens != 1 {
		t.Fatal(result)
	}
	if s.totals.GeneratedTokens != 8 || s.totals.Stalled != 1 || s.kvUsed != 0 {
		t.Fatalf("totals=%v kv=%d", s.totals, s.kvUsed)
	}
}
func TestAllPausedWaitAndStall(t *testing.T) {
	s, clk := testScheduler(t, Continuous, func(c *Config) { c.OutputBuffer = 1; c.StallTimeout = time.Second })
	r := submit(t, s, "slow", 3)
	step(t, s)
	step(t, s)
	if !s.allPaused() {
		t.Fatal("expected all paused")
	}
	before := clk.Now()
	step(t, s)
	if clk.Now() != before {
		t.Fatal("paused request used decode time")
	}
	_ = clk.Sleep(context.Background(), time.Second)
	step(t, s)
	if result := <-r.Result; result.Reason != "stalled" {
		t.Fatal(result)
	}
}
func TestDeterministic(t *testing.T) {
	run := func() View {
		s, _ := testScheduler(t, Continuous, nil)
		for i, n := range []int{2, 6, 2, 1, 7} {
			submit(t, s, string(rune('A'+i)), n)
		}
		finish(t, s)
		q := query{trace: true, reply: make(chan View, 1)}
		s.answer(q)
		return <-q.reply
	}
	if a, b := run(), run(); !reflect.DeepEqual(a, b) {
		t.Fatalf("different results:\n%+v\n%+v", a, b)
	}
}
