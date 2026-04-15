package pool

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/matmazurk/oidc-keepie/job"
)

var ErrPoolClosed = errors.New("pool is closed")

type Job struct {
	Payload job.Job
	Commit  func() error
}

type Pool struct {
	handler func(context.Context, job.Job)
	jobs    chan Job
	wg      sync.WaitGroup
	mu      sync.RWMutex
	closed  bool
}

func New(size int, handler func(context.Context, job.Job)) *Pool {
	p := &Pool{
		handler: handler,
		jobs:    make(chan Job, size),
	}
	p.wg.Add(size)
	for range size {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for j := range p.jobs {
		if err := j.Commit(); err != nil {
			slog.Error("committing offset", slog.String("error", err.Error()))
			continue
		}
		p.handler(context.Background(), j.Payload)
	}
}

func (p *Pool) Submit(ctx context.Context, j Job) error {
	// RLock is held for the duration of the send so that Close (which takes
	// the write lock) waits for all in-flight submits to finish before
	// closing the jobs channel — otherwise a concurrent Close could close
	// the channel while Submit is about to send on it.
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrPoolClosed
	}

	select {
	case p.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	close(p.jobs)
	p.wg.Wait()
}
