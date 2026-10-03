package model

import (
	"context"
	"fmt"
	"time"

	"continuous-batching-lab/internal/clock"
)

type Sequence struct {
	ID   string
	Next int
}
type Token struct {
	Index int    `json:"index"`
	Text  string `json:"text"`
}
type Costs struct {
	PrefillBase, PrefillPerToken time.Duration
	StepBase, StepPerActive      time.Duration
}

func DefaultCosts() Costs {
	return Costs{2 * time.Millisecond, 100 * time.Microsecond, 20 * time.Millisecond, 300 * time.Microsecond}
}

type Model interface {
	Prefill(context.Context, int) error
	DecodeStep(context.Context, []Sequence) ([]Token, error)
}

type Fake struct {
	Clock clock.Clock
	Costs Costs
}

func (m *Fake) Prefill(ctx context.Context, promptTokens int) error {
	return m.Clock.Sleep(ctx, m.Costs.PrefillBase+m.Costs.PrefillPerToken*time.Duration(promptTokens))
}
func (m *Fake) DecodeStep(ctx context.Context, batch []Sequence) ([]Token, error) {
	if err := m.Clock.Sleep(ctx, m.Costs.StepBase+m.Costs.StepPerActive*time.Duration(len(batch))); err != nil {
		return nil, err
	}
	tokens := make([]Token, len(batch))
	for i, seq := range batch {
		tokens[i] = Token{seq.Next, fmt.Sprintf("%s:%d", seq.ID, seq.Next)}
	}
	return tokens, nil
}
