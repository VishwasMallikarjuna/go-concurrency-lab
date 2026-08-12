package workerpool

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("worker pool is closed")
	ErrFull   = errors.New("worker pool queue is full")
)

type Job func(context.Context) error

type Pool struct {
	ctx    context.Context
	cancel context.CancelFunc

	jobs chan Job

	workers int
	wg      sync.WaitGroup

	mu     sync.RWMutex
	closed bool
}

func New(parent context.Context, workers, queueSize int) *Pool {
	ctx, cancel := context.WithCancel(parent)

	p := &Pool{
		ctx:     ctx,
		cancel:  cancel,
		jobs:    make(chan Job, queueSize),
		workers: workers,
	}

	p.start()

	return p
}

func (p *Pool) start() {
	p.wg.Add(p.workers)

	for i := 0; i < p.workers; i++ {
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return

		case job, ok := <-p.jobs:
			if !ok {
				return
			}

			p.execute(job)
		}
	}
}

func (p *Pool) execute(job Job) {
	defer func() {
		_ = recover()
	}()

	_ = job(p.ctx)
}

func (p *Pool) Submit(job Job) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrClosed
	}

	select {
	case p.jobs <- job:
		return nil

	default:
		return ErrFull
	}
}

func (p *Pool) Shutdown() {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return
	}

	p.closed = true
	close(p.jobs)

	p.mu.Unlock()

	p.wg.Wait()
	p.cancel()
}
