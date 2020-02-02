package slidingwindow

import (
	"log"
	"time"
)

// Datastore represents the central datastore.
type Datastore interface {
	// Add adds delta to the count of the window represented
	// by start, and returns the new count.
	Add(key string, start, delta int64) (int64, error)

	// Get returns the count of the window represented by start.
	Get(key string, start int64) (int64, error)
}

// syncHelper is a helper that will be leveraged by both BlockingSynchronizer
// and NonblockingSynchronizer.
type syncHelper struct {
	store        Datastore
	syncInterval time.Duration

	inProgress bool // Whether the synchronization is in progress.
	lastSynced time.Time
}

func newSyncHelper(store Datastore, syncInterval time.Duration) *syncHelper {
	return &syncHelper{store: store, syncInterval: syncInterval}
}

// IsTimeUp returns whether it's time to sync data to the central datastore.
func (h *syncHelper) IsTimeUp(now time.Time) bool {
	return !h.inProgress && now.Sub(h.lastSynced) >= h.syncInterval
}

func (h *syncHelper) InProgress() bool {
	return h.inProgress
}

func (h *syncHelper) Begin(now time.Time) {
	h.inProgress = true
	h.lastSynced = now
}

func (h *syncHelper) End() {
	h.inProgress = false
}

func (h *syncHelper) Sync(req SyncRequest) (resp SyncResponse, err error) {
	var newCount int64

	if req.Changes > 0 {
		newCount, err = h.store.Add(req.Key, req.Start, req.Changes)
	} else {
		newCount, err = h.store.Get(req.Key, req.Start)
	}

	if err != nil {
		return SyncResponse{}, err
	}

	return SyncResponse{
		OK:           true,
		Start:        req.Start,
		Changes:      req.Changes,
		OtherChanges: newCount - req.Count,
	}, nil
}

// BlockingSynchronizer does synchronization in a blocking mode and consumes
// no extra goroutine.
//
// It's recommended to use BlockingSynchronizer in low-concurrency scenarios,
// either for higher accuracy, or for less goroutine consumption.
type BlockingSynchronizer struct {
	helper *syncHelper
}

func NewBlockingSynchronizer(store Datastore, syncInterval time.Duration) *BlockingSynchronizer {
	return &BlockingSynchronizer{
		helper: newSyncHelper(store, syncInterval),
	}
}

func (s *BlockingSynchronizer) Start() {}

func (s *BlockingSynchronizer) Stop() {}

// Sync sends the window's count to the central datastore, and then update
// the window's count according to the response from the datastore.
func (s *BlockingSynchronizer) Sync(now time.Time, makeReq MakeFunc, handleResp HandleFunc) {
	if s.helper.IsTimeUp(now) {
		s.helper.Begin(now)

		resp, err := s.helper.Sync(makeReq())
		if err != nil {
			log.Printf("err: %v\n", err)
		}

		handleResp(resp)
		s.helper.End()
	}
}

// NonblockingSynchronizer does synchronization in a non-blocking mode. To achieve
// this, it needs to spawn a goroutine to exchange data with the central datastore.
//
// It's recommended to always use NonblockingSynchronizer in high-concurrency scenarios.
type NonblockingSynchronizer struct {
	reqC  chan SyncRequest
	respC chan SyncResponse

	stopC chan struct{}
	exitC chan struct{}

	helper *syncHelper
}

func NewNonblockingSynchronizer(store Datastore, syncInterval time.Duration) *NonblockingSynchronizer {
	return &NonblockingSynchronizer{
		reqC:   make(chan SyncRequest),
		respC:  make(chan SyncResponse),
		stopC:  make(chan struct{}),
		exitC:  make(chan struct{}),
		helper: newSyncHelper(store, syncInterval),
	}
}

func (s *NonblockingSynchronizer) Start() {
	go s.syncLoop()
}

func (s *NonblockingSynchronizer) Stop() {
	close(s.stopC)
	<-s.exitC
}

// syncLoop is a worker that receives a sync request and generates the
// corresponding sync response.
func (s *NonblockingSynchronizer) syncLoop() {
	for {
		select {
		case req := <-s.reqC:
			resp, err := s.helper.Sync(req)
			if err != nil {
				log.Printf("err: %v\n", err)
			}

			select {
			case s.respC <- resp:
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

// Sync tries to send the window's count to the central datastore, or to update
// the window's count according to the response from the latest synchronization.
// Since the exchange with the datastore is always slower than the execution of Sync,
// usually Sync must be called at least twice to update the window's count finally.
func (s *NonblockingSynchronizer) Sync(now time.Time, makeReq MakeFunc, handleResp HandleFunc) {
	if s.helper.IsTimeUp(now) {
		// Just try to sync. If this fails, we assume the previous synchronization
		// is still ongoing, and we wait for the next time.
		select {
		case s.reqC <- makeReq():
			s.helper.Begin(now)
		default:
		}
	}

	if s.helper.InProgress() {
		// Try to get the response from the latest synchronization.
		select {
		case resp := <-s.respC:
			handleResp(resp)
			s.helper.End()
		default:
		}
	}
}
