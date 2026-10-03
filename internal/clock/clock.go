package clock

import (
	"context"
	"time"
)

type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

type Real struct{}

func (Real) Now() time.Time { return time.Now() }
func (Real) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type Fake struct{ Time time.Time }

func NewFake() *Fake           { return &Fake{Time: time.Unix(0, 0).UTC()} }
func (f *Fake) Now() time.Time { return f.Time }
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.Time = f.Time.Add(d)
	return nil
}
