package mcpapp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestFixedWindowLimiterIsConcurrentAndIsolated(t *testing.T) {
	limiter, err := NewFixedWindowLimiter(10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed, denied := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, callErr := limiter.Allow(context.Background(), "client/tool", 1)
			mu.Lock()
			defer mu.Unlock()
			if callErr == nil {
				allowed++
			} else if errors.Is(callErr, ErrQuotaExceeded) {
				denied++
			} else {
				t.Errorf("unexpected limiter error: %v", callErr)
			}
		}()
	}
	wg.Wait()
	if allowed != 10 || denied != 10 {
		t.Fatalf("allowed=%d denied=%d", allowed, denied)
	}
	if _, err := limiter.Allow(context.Background(), "client/other-tool", 10); err != nil {
		t.Fatalf("capability keys should be isolated: %v", err)
	}
}
