package workerpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_ExecutesJobs(t *testing.T) {
	pool := New(context.Background(), 3, 10)

	var executed atomic.Int32

	for i := 0; i < 10; i++ {
		err := pool.Submit(func(ctx context.Context) error {
			executed.Add(1)
			return nil
		})

		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	pool.Shutdown()

	if got := executed.Load(); got != 10 {
		t.Fatalf("executed = %d, want 10", got)
	}
}

func TestPool_ExecutesConcurrently(t *testing.T) {
	pool := New(context.Background(), 3, 10)
	defer pool.Shutdown()

	started := make(chan struct{}, 3)
	release := make(chan struct{})

	for i := 0; i < 3; i++ {
		err := pool.Submit(func(ctx context.Context) error {
			started <- struct{}{}

			<-release

			return nil
		})

		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not execute concurrently")
		}
	}

	close(release)
}

func TestPool_QueueFull(t *testing.T) {
	pool := New(context.Background(), 1, 2)
	defer pool.Shutdown()

	block := make(chan struct{})

	job := func(ctx context.Context) error {
		<-block
		return nil
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("first Submit() = %v", err)
	}

	if err := pool.Submit(job); err != nil {
		t.Fatalf("second Submit() = %v", err)
	}

	if err := pool.Submit(job); err != ErrFull {
		t.Fatalf("third Submit() = %v, want %v", err, ErrFull)
	}

	close(block)
}

func TestPool_Shutdown(t *testing.T) {
	pool := New(context.Background(), 3, 10)

	var executed atomic.Int32

	for i := 0; i < 10; i++ {
		err := pool.Submit(func(ctx context.Context) error {
			executed.Add(1)
			return nil
		})

		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	pool.Shutdown()

	if got := executed.Load(); got != 10 {
		t.Fatalf("executed = %d, want 10", got)
	}

	if err := pool.Submit(func(ctx context.Context) error {
		return nil
	}); err != ErrClosed {
		t.Fatalf("Submit() after shutdown = %v, want %v", err, ErrClosed)
	}
}

func TestPool_RecoversFromJobPanic(t *testing.T) {
	pool := New(context.Background(), 1, 10)
	defer pool.Shutdown()

	panicJob := func(ctx context.Context) error {
		panic("boom")
	}

	normalJob := func(ctx context.Context) error {
		return nil
	}

	if err := pool.Submit(panicJob); err != nil {
		t.Fatal(err)
	}

	if err := pool.Submit(normalJob); err != nil {
		t.Fatal(err)
	}

	pool.Shutdown()
}

func TestPool_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	pool := New(ctx, 1, 10)

	cancel()

	done := make(chan struct{})

	err := pool.Submit(func(ctx context.Context) error {
		close(done)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
		t.Fatal("job executed after cancellation")

	case <-time.After(100 * time.Millisecond):
		// expected
	}

	pool.Shutdown()
}

func TestPool_ConcurrentSubmitAndShutdown(t *testing.T) {
	pool := New(context.Background(), 4, 100)

	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				_ = pool.Submit(func(ctx context.Context) error {
					return nil
				})
			}
		}()
	}

	go pool.Shutdown()

	wg.Wait()
}

func BenchmarkPool_Submit(b *testing.B) {
	pool := New(context.Background(), 10, b.N)
	defer pool.Shutdown()

	job := func(ctx context.Context) error {
		return nil
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = pool.Submit(job)
	}
}
