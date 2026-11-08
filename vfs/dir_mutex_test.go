package vfs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDirMutexZeroValueAndCancellation(t *testing.T) {
	var mu dirMutex
	mu.RLock()
	mu.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, mu.LockContext(ctx), context.Canceled)
	mu.RUnlock()
	mu.RUnlock()

	mu.Lock()
	read := make(chan struct{})
	go func() {
		mu.RLock()
		mu.RUnlock()
		close(read)
	}()
	select {
	case <-read:
		t.Error("reader entered while writer owned the lock")
	case <-time.After(20 * time.Millisecond):
	}
	mu.Unlock()
	select {
	case <-read:
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not acquire released lock")
	}
	require.NoError(t, mu.LockContext(context.Background()))
	mu.Unlock()
}
