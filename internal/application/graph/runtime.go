package graphapp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type SystemClock = systemClock
type RandomIDs = randomIDs

// Run executes the configured number of independent claim loops. Global,
// tenant and repository limits are still enforced by the PostgreSQL claim,
// so adding role replicas cannot bypass the configured deployment budget.
func (w *Worker) Run(ctx context.Context, owner string, idleDelay, failureDelay time.Duration) error {
	if w == nil || strings.TrimSpace(owner) == "" || owner != strings.TrimSpace(owner) || idleDelay <= 0 || failureDelay <= 0 {
		return fmt.Errorf("graph worker runtime requires an exact owner and positive polling delays")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, w.config.MaxConcurrentGlobal)
	for slot := 0; slot < w.config.MaxConcurrentGlobal; slot++ {
		slotOwner := owner
		if w.config.MaxConcurrentGlobal > 1 {
			slotOwner += ":" + strconv.Itoa(slot+1)
		}
		go func() { results <- w.runLoop(runCtx, slotOwner, idleDelay, failureDelay) }()
	}
	var failures []error
	for slot := 0; slot < w.config.MaxConcurrentGlobal; slot++ {
		if err := <-results; err != nil && !errors.Is(err, context.Canceled) {
			failures = append(failures, err)
		}
		cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(failures...)
}

func (w *Worker) runLoop(ctx context.Context, owner string, idleDelay, failureDelay time.Duration) error {
	for {
		processed, err := w.RunOnce(ctx, owner)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay := idleDelay
		if err != nil {
			delay = failureDelay
		} else if processed {
			continue
		}
		if err := waitGraphRuntime(ctx, delay); err != nil {
			return err
		}
	}
}

func (d *GraphOutboxDispatcher) Run(ctx context.Context, interval time.Duration) error {
	if d == nil || interval <= 0 {
		return fmt.Errorf("graph outbox runtime requires a positive interval")
	}
	for {
		_, _ = d.DispatchOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitGraphRuntime(ctx, interval); err != nil {
			return err
		}
	}
}

func (r *Reconciler) Run(ctx context.Context, interval time.Duration) error {
	if r == nil || interval <= 0 {
		return fmt.Errorf("graph reconciler runtime requires a positive interval")
	}
	for {
		_, _ = r.RunOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitGraphRuntime(ctx, interval); err != nil {
			return err
		}
	}
}

func waitGraphRuntime(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
