package pool_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matmazurk/oidc-keepie/job"
	"github.com/matmazurk/oidc-keepie/pool"
)

func TestNew(t *testing.T) {
	var called atomic.Int64
	p := pool.New(2, func(ctx context.Context, j job.Job) {
		called.Add(1)
	})
	defer p.Close()

	j := job.MustNew("job-1", "http://example.com", time.Now())
	err := p.Submit(context.Background(), pool.Job{
		Payload: j,
		Commit:  func() error { return nil },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if called.Load() != 1 {
		t.Errorf("expected handler called 1 time, got %d", called.Load())
	}
}

func TestCommitCalledBeforeHandler(t *testing.T) {
	var order []string
	var mu sync.Mutex

	p := pool.New(1, func(ctx context.Context, j job.Job) {
		mu.Lock()
		order = append(order, "handler")
		mu.Unlock()
	})
	defer p.Close()

	j := job.MustNew("job-1", "http://example.com", time.Now())
	err := p.Submit(context.Background(), pool.Job{
		Payload: j,
		Commit: func() error {
			mu.Lock()
			order = append(order, "commit")
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "commit" || order[1] != "handler" {
		t.Errorf("expected [commit, handler], got %v", order)
	}
}

func TestCommitErrorSkipsHandler(t *testing.T) {
	var handlerCalled atomic.Int64

	p := pool.New(1, func(ctx context.Context, j job.Job) {
		handlerCalled.Add(1)
	})
	defer p.Close()

	j := job.MustNew("job-1", "http://example.com", time.Now())
	err := p.Submit(context.Background(), pool.Job{
		Payload: j,
		Commit:  func() error { return errors.New("commit failed") },
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if handlerCalled.Load() != 0 {
		t.Error("handler should not be called when commit fails")
	}
}

func TestSubmitOnClosedPool(t *testing.T) {
	p := pool.New(1, func(ctx context.Context, j job.Job) {})
	p.Close()

	j := job.MustNew("job-1", "http://example.com", time.Now())
	err := p.Submit(context.Background(), pool.Job{
		Payload: j,
		Commit:  func() error { return nil },
	})
	if err != pool.ErrPoolClosed {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

func TestCloseWaitsForInFlight(t *testing.T) {
	var completed atomic.Int64

	p := pool.New(2, func(ctx context.Context, j job.Job) {
		time.Sleep(200 * time.Millisecond)
		completed.Add(1)
	})

	for i := range 2 {
		j := job.MustNew(fmt.Sprintf("job-%d", i), "http://example.com", time.Now())
		if err := p.Submit(context.Background(), pool.Job{
			Payload: j,
			Commit:  func() error { return nil },
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	p.Close()

	if completed.Load() != 2 {
		t.Errorf("expected 2 completed jobs after Close, got %d", completed.Load())
	}
}

func TestSubmitRespectsContextCancellation(t *testing.T) {
	done := make(chan struct{})
	p := pool.New(1, func(ctx context.Context, j job.Job) {
		<-done // block until test is done
	})

	commit := func() error { return nil }
	j := job.MustNew("job-1", "http://example.com", time.Now())

	// fill worker
	if err := p.Submit(context.Background(), pool.Job{Payload: j, Commit: commit}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// fill buffer
	if err := p.Submit(context.Background(), pool.Job{Payload: j, Commit: commit}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}

	// third submit should block, then unblock on cancel
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := p.Submit(ctx, pool.Job{Payload: j, Commit: commit})
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	close(done)
	p.Close()
}
