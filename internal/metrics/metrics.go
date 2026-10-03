package metrics

import (
	"math"
	"sort"
	"time"
)

type Percentiles struct {
	Samples int     `json:"samples"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
}

func Quantiles(values []float64) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	x := append([]float64(nil), values...)
	sort.Float64s(x)
	at := func(p float64) float64 { return x[int(math.Ceil(p*float64(len(x))))-1] }
	return Percentiles{len(x), at(.50), at(.95), at(.99)}
}

type Totals struct {
	Submitted       uint64 `json:"submitted"`
	Completed       uint64 `json:"completed"`
	Cancelled       uint64 `json:"cancelled"`
	Stalled         uint64 `json:"stalled"`
	GeneratedTokens uint64 `json:"generated_tokens"`
	DeliveredTokens uint64 `json:"delivered_tokens"`
	DecodeSteps     uint64 `json:"decode_steps"`
}
type Snapshot struct {
	Policy     string      `json:"policy"`
	Waiting    int         `json:"waiting"`
	Ready      int         `json:"ready"`
	Active     int         `json:"active"`
	Paused     int         `json:"paused"`
	KVUsed     int         `json:"kv_budget_used"`
	KVLimit    int         `json:"kv_token_budget"`
	Totals     Totals      `json:"totals"`
	TTFT       Percentiles `json:"ttft_ms"`
	Completion Percentiles `json:"completion_ms"`
}

const window = 10000

type Samples struct {
	Values []float64
	next   int
}

func (s *Samples) Observe(d time.Duration) {
	v := float64(d) / float64(time.Millisecond)
	if len(s.Values) < window {
		s.Values = append(s.Values, v)
		return
	}
	s.Values[s.next] = v
	s.next = (s.next + 1) % window
}
func (s *Samples) Percentiles() Percentiles { return Quantiles(s.Values) }
