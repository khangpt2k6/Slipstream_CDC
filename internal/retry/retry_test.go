package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
)

// fastCfg keeps delays sub-millisecond so the tests run instantly.
var fastCfg = retry.Config{Base: time.Millisecond, Max: 4 * time.Millisecond}

func TestDoSucceedsImmediately(t *testing.T) {
	calls := 0
	err := retry.Do(context.Background(), fastCfg, nil, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1 (no retries on first success)", calls)
	}
}

func TestDoSucceedsAfterNFailures(t *testing.T) {
	want := errors.New("transient")
	calls, retries := 0, 0
	err := retry.Do(context.Background(), fastCfg,
		func(attempt int, _ time.Duration, gotErr error) {
			retries++
			if !errors.Is(gotErr, want) {
				t.Errorf("onRetry err = %v, want %v", gotErr, want)
			}
			if attempt != retries {
				t.Errorf("onRetry attempt = %d, want %d", attempt, retries)
			}
		},
		func() error {
			calls++
			if calls < 3 {
				return want
			}
			return nil
		})
	if err != nil {
		t.Fatalf("Do() = %v, want nil after recovery", err)
	}
	if calls != 3 {
		t.Errorf("fn called %d times, want 3 (2 failures then success)", calls)
	}
	if retries != 2 {
		t.Errorf("onRetry called %d times, want 2", retries)
	}
}

func TestDoReturnsCtxErrWhenCancelledMidBackoff(t *testing.T) {
	// A long Base ensures the test is parked in the backoff sleep when the ctx is
	// cancelled, proving Do unblocks promptly rather than waiting out the delay.
	cfg := retry.Config{Base: time.Hour, Max: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	go func() {
		// Give Do time to enter its first backoff, then cancel.
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := retry.Do(ctx, cfg, nil, func() error {
		calls++
		return errors.New("always fails")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Do() took %v to unblock, want prompt return on cancel", elapsed)
	}
	if calls != 1 {
		t.Errorf("fn called %d times, want 1 (cancelled during first backoff)", calls)
	}
}

func TestDoReturnsCtxErrWhenAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := retry.Do(ctx, fastCfg, nil, func() error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() = %v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Errorf("fn called %d times, want 0 (ctx already cancelled)", calls)
	}
}
