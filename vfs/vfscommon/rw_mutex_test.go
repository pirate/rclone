package vfscommon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRWMutexReadCancellation(t *testing.T) {
	var mu RWMutex
	mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- mu.RLockContext(ctx) }()
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reader did not cancel while writer held the lock")
	}
	mu.Unlock()
	require.NoError(t, mu.RLockContext(context.Background()))
	mu.RUnlock()
}

func TestRWMutexCond(t *testing.T) {
	var mu RWMutex
	cond := sync.NewCond(&mu)
	ready, finished := make(chan struct{}), make(chan struct{})
	go func() {
		mu.Lock()
		close(ready)
		cond.Wait()
		mu.Unlock()
		close(finished)
	}()
	<-ready
	mu.Lock()
	cond.Signal()
	mu.Unlock()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("condition waiter did not reacquire the lock")
	}
}
