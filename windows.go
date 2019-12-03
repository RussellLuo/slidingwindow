package slidingwindow

import (
	"log"
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

func (w *LocalWindow) Sync(now time.Time) {
}

// SyncWindow represents a window that will sync counter data to the
// central datastore asynchronously.
//
// Note that for the best coordination between the window and the synchronizer,
// the synchronization is not automatic but is driven by the call to Sync. Also
// there are two sync modes: blocking-sync and non-blocking-sync. In low-concurrency
// scenarios, you can use the blocking-sync mode for higher accuracy. In other
// cases (e.g. accuracy is less important than performance, or in high-concurrency
// scenarios), choose the non-blocking-sync mode instead.
type SyncWindow struct {
	LocalWindow
	changes int64

	syncer       *synchronizer
	key          string
	blockingSync bool
	syncInterval time.Duration

	lastSynced time.Time
}

// NewSyncWindow creates an instance of SyncWindow with the given configurations.
// Particularly, blockingSync indicates whether to block waiting for each
// synchronization to complete.
func NewSyncWindow(store Datastore, key string, blockingSync bool, syncInterval time.Duration) (Window, StopFunc) {
	w := &SyncWindow{
		syncer:       newSynchronizer(store),
		key:          key,
		blockingSync: blockingSync,
		syncInterval: syncInterval,
	}

	w.syncer.Start()
	return w, w.syncer.Stop
}

func (w *SyncWindow) AddCount(n int64) {
	w.changes += n
	w.LocalWindow.AddCount(n)
}

func (w *SyncWindow) Reset(s time.Time, c int64) {
	// Clear changes accumulated within the OLD window.
	//
	// Note that for simplicity, we do not sync remaining changes to the
	// central datastore before the reset, thus let the periodic synchronization
	// take full charge of the accuracy of the window's count.
	w.changes = 0

	w.LocalWindow.Reset(s, c)
}

func (w *SyncWindow) makeSyncIn() syncIn {
	return syncIn{
		key:     w.key,
		start:   w.LocalWindow.start,
		count:   w.LocalWindow.count,
		changes: w.changes,
	}
}

func (w *SyncWindow) handleSyncOut(out syncOut) {
	if out.ok && out.start == w.LocalWindow.start {
		// Update the state of the window, only when it has not been reset
		// during the latest sync.

		// Take the changes accumulated by other limiters into consideration.
		w.LocalWindow.count += out.otherChanges

		// Subtract the amount that has been synced from existing changes.
		w.changes -= out.changes
	}
}

// blockSync does syncing in blocking mode.
func (w *SyncWindow) blockSync(now time.Time) {
	if now.Sub(w.lastSynced) >= w.syncInterval {
		// It's time to sync the existing counter to the central datastore.

		w.syncer.InC <- w.makeSyncIn()
		w.lastSynced = now

		w.handleSyncOut(<-w.syncer.OutC)
	}
}

// nonblockSync tries to sync the window's count to the central datastore, or
// to update the window's count according to the result from the latest sync.
// Since the exchange with the datastore is always slower than the execution of
// nonblockSync, usually nonblockSync must be called at least twice to update
// the window's count finally.
func (w *SyncWindow) nonblockSync(now time.Time) {
	if now.Sub(w.lastSynced) >= w.syncInterval {
		// It's time to sync the existing counter to the central datastore.

		// Just try to sync. If this fails, we assume the previous sync is
		// still ongoing, and we wait for the next time.
		select {
		case w.syncer.InC <- w.makeSyncIn():
			w.lastSynced = now
		default:
		}
	}

	// Try to get the result from the latest sync.
	select {
	case out := <-w.syncer.OutC:
		w.handleSyncOut(out)
	default:
	}
}

func (w *SyncWindow) Sync(now time.Time) {
	if w.blockingSync {
		w.blockSync(now)
	} else {
		w.nonblockSync(now)
	}
}

type Datastore interface {
	// Add adds delta to the count of the window represented
	// by start, and returns the new count.
	Add(key string, start, delta int64) (int64, error)

	// Get returns the count of the window represented by start.
	Get(key string, start int64) (int64, error)
}

type syncIn struct {
	key     string
	start   int64
	count   int64
	changes int64
}

type syncOut struct {
	// Whether the sync succeeds.
	ok bool

	start int64
	// The changes accumulated by the local limiter.
	changes int64
	// The total changes accumulated by all the other limiters.
	otherChanges int64
}

type synchronizer struct {
	InC  chan syncIn
	OutC chan syncOut

	stopC chan struct{}
	exitC chan struct{}

	store Datastore
}

func newSynchronizer(store Datastore) *synchronizer {
	return &synchronizer{
		InC:   make(chan syncIn),
		OutC:  make(chan syncOut),
		stopC: make(chan struct{}),
		exitC: make(chan struct{}),
		store: store,
	}
}

// Start starts the sync loop.
func (s *synchronizer) Start() {
	go s.syncLoop(s.store)
}

// Stop stops the sync loop and waits for it to exit.
func (s *synchronizer) Stop() {
	close(s.stopC)
	<-s.exitC
}

func (s *synchronizer) syncLoop(store Datastore) {
	var (
		newCount int64
		err      error
	)

	for {
		select {
		case in := <-s.InC:
			if in.changes > 0 {
				newCount, err = store.Add(in.key, in.start, in.changes)
			} else {
				newCount, err = store.Get(in.key, in.start)
			}

			out := syncOut{}
			if err != nil {
				log.Printf("err: %v\n", err)
			} else {
				out.ok = true
				out.start = in.start
				out.changes = in.changes
				out.otherChanges = newCount - in.count
			}

			select {
			case s.OutC <- out:
			case <-s.stopC:
				goto exit
			}
		case <-s.stopC:
			goto exit
		}
	}

exit:
	close(s.exitC)
}
