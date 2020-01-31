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

type SyncRequest struct {
	Key     string
	Start   int64
	Count   int64
	Changes int64
}

type SyncResponse struct {
	// Whether the synchronization succeeds.
	OK    bool
	Start int64
	// The changes accumulated by the local limiter.
	Changes int64
	// The total changes accumulated by all the other limiters.
	OtherChanges int64
}

type MakeFunc func() SyncRequest
type HandleFunc func(SyncResponse)

type Synchronizer interface {
	// Start starts the synchronization goroutine, if any.
	Start()

	// Stop stops the synchronization goroutine, if any, and waits for it to exit.
	Stop()

	// Sync sends a synchronization request.
	Sync(time.Time, MakeFunc, HandleFunc)
}

// syncHelper is a helper that will be leveraged by both BlockingSynchronizer
// and NonblockingSynchronizer.
type syncHelper struct {
	store Datastore

	syncInterval time.Duration
	inProgress   bool
	lastSynced   time.Time
}

func (h *syncHelper) begin(now time.Time) {
	h.inProgress = true
	h.lastSynced = now
}

func (h *syncHelper) end() {
	h.inProgress = false
}

// isTimeUp returns whether it's time to sync data to the central datastore.
func (h *syncHelper) isTimeUp(now time.Time) bool {
	return !h.inProgress && now.Sub(h.lastSynced) >= h.syncInterval
}

func (h *syncHelper) sync(req SyncRequest) (resp SyncResponse, err error) {
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
		helper: &syncHelper{store: store, syncInterval: syncInterval},
	}
}

func (s *BlockingSynchronizer) Start() {}

func (s *BlockingSynchronizer) Stop() {}

// Sync sends the window's count to the central datastore, and then update
// the window's count according to the response from the datastore.
func (s *BlockingSynchronizer) Sync(now time.Time, makeReq MakeFunc, handleResp HandleFunc) {
	if s.helper.isTimeUp(now) {
		resp, err := s.helper.sync(makeReq())
		if err != nil {
			log.Printf("err: %v\n", err)
		}
		s.helper.begin(now)

		handleResp(resp)
		s.helper.end()
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
		helper: &syncHelper{store: store, syncInterval: syncInterval},
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
			resp, err := s.helper.sync(req)
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

// Sync tries to sync the window's count to the central datastore, or to update
// the window's count according to the response from the latest synchronization.
// Since the exchange with the datastore is always slower than the execution of Sync,
// usually Sync must be called at least twice to update the window's count finally.
func (s *NonblockingSynchronizer) Sync(now time.Time, makeReq MakeFunc, handleResp HandleFunc) {
	if s.helper.isTimeUp(now) {
		// Just try to sync. If this fails, we assume the previous synchronization
		// is still ongoing, and we wait for the next time.
		select {
		case s.reqC <- makeReq():
			s.helper.begin(now)
		default:
		}
	}

	if s.helper.inProgress {
		// Try to get the response from the latest synchronization.
		select {
		case resp := <-s.respC:
			handleResp(resp)
			s.helper.end()
		default:
		}
	}
}
