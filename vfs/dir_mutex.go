package vfs

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

const dirMutexReaders = 1 << 30

// dirMutex is a reader/writer lock with cancellable writer acquisition.
// Its zero value is ready to use. It must not be copied after first use.
// Taking every permit excludes readers; semaphore's FIFO queue prevents
// new readers from starving a waiting writer.
type dirMutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *dirMutex) tokens() *semaphore.Weighted {
	m.once.Do(func() { m.sem = semaphore.NewWeighted(dirMutexReaders) })
	return m.sem
}

func (m *dirMutex) LockContext(ctx context.Context) error {
	return m.tokens().Acquire(ctx, dirMutexReaders)
}

func (m *dirMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *dirMutex) Unlock() {
	m.tokens().Release(dirMutexReaders)
}

func (m *dirMutex) RLock() {
	_ = m.tokens().Acquire(context.Background(), 1)
}

func (m *dirMutex) RUnlock() {
	m.tokens().Release(1)
}
