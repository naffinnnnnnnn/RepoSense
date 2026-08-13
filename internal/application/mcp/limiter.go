package mcpapp

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrQuotaExceeded = errors.New("quota exceeded")

type FixedWindowLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	now     func() time.Time
	buckets map[string]bucket
}
type bucket struct {
	start time.Time
	used  int
}

func NewFixedWindowLimiter(limit int, window time.Duration) (*FixedWindowLimiter, error) {
	if limit <= 0 || window <= 0 {
		return nil, errors.New("rate limit and window must be positive")
	}
	return &FixedWindowLimiter{limit: limit, window: window, now: time.Now, buckets: map[string]bucket{}}, nil
}

func (l *FixedWindowLimiter) Allow(ctx context.Context, key string, cost int) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if key == "" || cost <= 0 {
		return 0, errors.New("rate-limit key and positive cost are required")
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b.start.IsZero() || now.Sub(b.start) >= l.window {
		b = bucket{start: now}
	}
	if b.used+cost > l.limit {
		retry := l.window - now.Sub(b.start)
		if retry < 0 {
			retry = 0
		}
		return retry, ErrQuotaExceeded
	}
	b.used += cost
	l.buckets[key] = b
	return 0, nil
}
