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
// central datascore asynchronously.
type SyncWindow struct {
	base LocalWindow

	changes int64
	addC    chan int64
	resetC  chan int64

	key   string
	store Datastore
}

func NewSyncWindow(key string, store Datastore, syncInterval time.Duration) (Window, StopFunc) {
	w := &SyncWindow{
		addC:   make(chan int64),
		resetC: make(chan int64),
		key:    key,
		store:  store,
	}

	stopC := make(chan struct{})
	exitC := make(chan struct{})

	// Start the sync loop.
	go w.syncLoop(syncInterval, stopC, exitC)

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
	w.addC <- n
	atomic.AddInt64(&w.base.count, n)
}

func (w *SyncWindow) Reset(s time.Time, c int64) {
	w.resetC <- atomic.LoadInt64(&w.base.start)

	atomic.StoreInt64(&w.base.start, s.UnixNano())
	atomic.StoreInt64(&w.base.count, c)
}

func (w *SyncWindow) syncLoop(interval time.Duration, stopC, exitC chan struct{}) {
	var (
		newCount   int64
		err        error
		syncTicker = time.NewTicker(interval)
	)

	for {
		select {
		case delta := <-w.addC:
			w.changes += delta
		case start := <-w.resetC:
			if w.changes > 0 {
				// Try to add remaining changes to the count of the existing
				// window represented by start.
				w.store.Add(w.key, start, w.changes)

				// Always reset changes regardless of possible errors.
				w.changes = 0
			}

		case <-syncTicker.C:
			start := atomic.LoadInt64(&w.base.start)
			if w.changes > 0 {
				if newCount, err = w.store.Add(w.key, start, w.changes); err != nil {
					log.Printf("err: %v\n", err)
					continue
				}
				w.changes = 0
			} else {
				if newCount, err = w.store.Get(w.key, start); err != nil {
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
