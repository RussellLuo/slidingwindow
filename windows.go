package slidingwindow

import (
	"log"
	"sync/atomic"
	"time"
)

// LocalWindow represents a window that ignores sync behavior entirely
// and only stores counters in memory.
type LocalWindow struct {
	// The start boundary (timestamp in nanoseconds) of the window.
	// [start, start + size)
	start int64

	// The total count of events happened in the window.
	count int64
}

func NewLocalWindow() (Window, StopFunc) {
	return &LocalWindow{}, func() {}
}

func (w *LocalWindow) Start() time.Time {
	return time.Unix(0, w.start)
}

func (w *LocalWindow) Count() int64 {
	return w.count
}

func (w *LocalWindow) AddCount(n int64) {
	w.count += n
}

func (w *LocalWindow) Reset(s time.Time, c int64) {
	w.start = s.UnixNano()
	w.count = c
}

type Datastore interface {
	// Add adds delta to the count of the window represented
	// by start, and returns the new count.
	Add(key string, start, delta int64) (int64, error)

	// Get returns the count of the window represented by start.
	Get(key string, start int64) (int64, error)
}

// SyncWindow represents a window that will sync counter data to the
// central datastore asynchronously.
type SyncWindow struct {
	base    LocalWindow
	changes int64
}

func NewSyncWindow(key string, store Datastore, syncInterval time.Duration) (Window, StopFunc) {
	w := &SyncWindow{}

	stopC := make(chan struct{})
	exitC := make(chan struct{})

	// Start the sync loop.
	go w.syncLoop(key, store, syncInterval, stopC, exitC)

	return w, func() {
		// Stop the sync loop and wait for it to exit.
		close(stopC)
		<-exitC
	}
}

func (w *SyncWindow) Start() time.Time {
	start := atomic.LoadInt64(&w.base.start)
	return time.Unix(0, start)
}

func (w *SyncWindow) Count() int64 {
	return atomic.LoadInt64(&w.base.count)
}

func (w *SyncWindow) AddCount(n int64) {
	atomic.AddInt64(&w.changes, n)
	atomic.AddInt64(&w.base.count, n)
}

func (w *SyncWindow) Reset(s time.Time, c int64) {
	// Clear changes accumulated within the OLD window.
	//
	// Note that for simplicity, we do not sync remaining changes to the
	// central datastore before the reset, thus let the periodic synchronization
	// take full charge of the accuracy of the window's count.
	atomic.StoreInt64(&w.changes, 0)

	atomic.StoreInt64(&w.base.start, s.UnixNano())
	atomic.StoreInt64(&w.base.count, c)
}

func (w *SyncWindow) syncLoop(key string, store Datastore, interval time.Duration, stopC, exitC chan struct{}) {
	var (
		newCount   int64
		err        error
		unsynced   = struct{ start, changes int64 }{}
		syncTicker = time.NewTicker(interval)
	)

	for {
		select {
		case <-syncTicker.C:
			start := atomic.LoadInt64(&w.base.start)
			if unsynced.start != start {
				// The window has been reset after the last synchronization.

				// The existing unsynced data is outdated, just clear it.
				unsynced.start = start
				unsynced.changes = 0
			}

			// We accumulate changes here because the existing unsynced
			// changes may be nonzero if the last synchronization fails.
			unsynced.changes += atomic.SwapInt64(&w.changes, 0)

			if unsynced.changes > 0 {
				if newCount, err = store.Add(key, unsynced.start, unsynced.changes); err != nil {
					log.Printf("err: %v\n", err)
					continue
				}
				unsynced.changes = 0
			} else {
				if newCount, err = store.Get(key, unsynced.start); err != nil {
					log.Printf("err: %v\n", err)
					continue
				}
			}

			atomic.StoreInt64(&w.base.count, newCount)
		case <-stopC:
			close(exitC)
			return
		}
	}
}
