package slidingwindow

import (
	"log"
	"sync/atomic"
	"time"
)

const (
	opAdd   = 1
	opReset = 2
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

// op is an operation that represents a state change of the window.
type op struct {
	typ  int   // opAdd or opReset
	data int64 // opAdd-> delta, opReset-> start
}

// SyncWindow represents a window that will sync counter data to the
// central datastore asynchronously.
type SyncWindow struct {
	base LocalWindow

	opC chan op
}

func NewSyncWindow(key string, store Datastore, syncInterval time.Duration) (Window, StopFunc) {
	w := &SyncWindow{
		opC: make(chan op),
	}

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
	w.opC <- op{typ: opAdd, data: n}

	atomic.AddInt64(&w.base.count, n)
}

func (w *SyncWindow) Reset(s time.Time, c int64) {
	w.opC <- op{typ: opReset, data: atomic.LoadInt64(&w.base.start)}

	atomic.StoreInt64(&w.base.start, s.UnixNano())
	atomic.StoreInt64(&w.base.count, c)
}

func (w *SyncWindow) syncLoop(key string, store Datastore, interval time.Duration, stopC, exitC chan struct{}) {
	var (
		newCount   int64
		err        error
		changes    int64
		syncTicker = time.NewTicker(interval)
	)

	for {
		select {
		case o := <-w.opC:
			// By using the same channel opC to receive both opAdd and opReset
			// signals, we can ensure the opReset signal, which always happens
			// before the opAdd signal during the reset of the window, to be
			// handled before the opAdd one.

			switch o.typ {
			case opAdd:
				delta := o.data
				changes += delta
			case opReset:
				// opReset is ensured to be handled before opAdd (see above
				// descriptions), so changes here must belong to the OLD window
				// before the reset.
				if changes > 0 {
					// Try to add remaining changes to the count of the OLD
					// window represented by start.
					start := o.data
					store.Add(key, start, changes) // nolint:errcheck

					// Always reset changes regardless of possible errors.
					changes = 0
				}
			}
		case <-syncTicker.C:
			start := atomic.LoadInt64(&w.base.start)
			if changes > 0 {
				if newCount, err = store.Add(key, start, changes); err != nil {
					log.Printf("err: %v\n", err)
					continue
				}
				changes = 0
			} else {
				if newCount, err = store.Get(key, start); err != nil {
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
