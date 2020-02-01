package slidingwindow

import (
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

func NewLocalWindow() (*LocalWindow, StopFunc) {
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

func (w *LocalWindow) Sync(now time.Time) {}

type (
	SyncRequest struct {
		Key     string
		Start   int64
		Count   int64
		Changes int64
	}

	SyncResponse struct {
		// Whether the synchronization succeeds.
		OK    bool
		Start int64
		// The changes accumulated by the local limiter.
		Changes int64
		// The total changes accumulated by all the other limiters.
		OtherChanges int64
	}

	MakeFunc   func() SyncRequest
	HandleFunc func(SyncResponse)
)

type Synchronizer interface {
	// Start starts the synchronization goroutine, if any.
	Start()

	// Stop stops the synchronization goroutine, if any, and waits for it to exit.
	Stop()

	// Sync sends a synchronization request.
	Sync(time.Time, MakeFunc, HandleFunc)
}

// SyncWindow represents a window that will sync counter data to the
// central datastore asynchronously.
//
// Note that for the best coordination between the window and the synchronizer,
// the synchronization is not automatic but is driven by the call to Sync.
type SyncWindow struct {
	LocalWindow
	changes int64

	key    string
	syncer Synchronizer
}

// NewSyncWindow creates an instance of SyncWindow with the given synchronizer.
func NewSyncWindow(key string, syncer Synchronizer) (*SyncWindow, StopFunc) {
	w := &SyncWindow{
		key:    key,
		syncer: syncer,
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

func (w *SyncWindow) makeSyncRequest() SyncRequest {
	return SyncRequest{
		Key:     w.key,
		Start:   w.LocalWindow.start,
		Count:   w.LocalWindow.count,
		Changes: w.changes,
	}
}

func (w *SyncWindow) handleSyncResponse(resp SyncResponse) {
	if resp.OK && resp.Start == w.LocalWindow.start {
		// Update the state of the window, only when it has not been reset
		// during the latest sync.

		// Take the changes accumulated by other limiters into consideration.
		w.LocalWindow.count += resp.OtherChanges

		// Subtract the amount that has been synced from existing changes.
		w.changes -= resp.Changes
	}
}

func (w *SyncWindow) Sync(now time.Time) {
	w.syncer.Sync(now, w.makeSyncRequest, w.handleSyncResponse)
}
