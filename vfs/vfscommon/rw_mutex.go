package vfscommon

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

const rwMutexReaders = 1 << 30

// RWMutex is a reader/writer lock with cancellable acquisition.
// Its zero value is ready to use. It must not be copied after first use.
// Taking every permit excludes readers; semaphore's FIFO queue prevents
// new readers from starving a waiting writer.
type RWMutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *RWMutex) tokens() *semaphore.Weighted {
	m.once.Do(func() { m.sem = semaphore.NewWeighted(rwMutexReaders) })
	return m.sem
}

// LockContext acquires the write lock or returns the context error.
func (m *RWMutex) LockContext(ctx context.Context) error {
	return m.tokens().Acquire(ctx, rwMutexReaders)
}

// Lock acquires the write lock.
func (m *RWMutex) Lock() {
	_ = m.LockContext(context.Background())
}

// Unlock releases the write lock.
func (m *RWMutex) Unlock() {
	m.tokens().Release(rwMutexReaders)
}

// RLockContext acquires a read lock or returns the context error.
func (m *RWMutex) RLockContext(ctx context.Context) error {
	return m.tokens().Acquire(ctx, 1)
}

// RLock acquires a read lock.
func (m *RWMutex) RLock() {
	_ = m.RLockContext(context.Background())
}

// RUnlock releases a read lock.
func (m *RWMutex) RUnlock() {
	m.tokens().Release(1)
}
