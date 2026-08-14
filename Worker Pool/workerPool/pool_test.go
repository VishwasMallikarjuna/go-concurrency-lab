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
