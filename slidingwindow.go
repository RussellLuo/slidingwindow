package slidingwindow

import (
	"sync"
	"time"
)

// Window represents a fixed-window.
type Window interface {
	// Start returns the start boundary.
	Start() time.Time

	// Count returns the accumulated count.
	Count() int64

	// AddCount increments the accumulated count by n.
	AddCount(n int64)

	// Reset sets the state of the window with the given settings.
	Reset(s time.Time, c int64)
}

// StopFunc cancel the window's sync behaviour.
type StopFunc func()

// NewWindow creates a new window, and returns a function to stop
// the possible sync behaviour within it.
type NewWindow func() (Window, StopFunc)

type Limiter struct {
	size  time.Duration
	limit int64

	mu sync.Mutex

	// The current window.
	curr Window
	// The previous window.
	prev Window

	exitC     chan struct{}
	waitGroup sync.WaitGroup
}

// NewLimiter creates a new limiter, and returns a function to stop
// the possible sync behaviour within its internal windows.
func NewLimiter(size time.Duration, limit int64, newWindow NewWindow) (*Limiter, StopFunc) {
	currWin, currStop := newWindow()
	prevWin, prevStop := newWindow()

	lim := &Limiter{
		size:  size,
		limit: limit,
		curr:  currWin,
		prev:  prevWin,
	}

	return lim, func() {
		currStop()
		prevStop()
	}
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// AllowN reports whether n events may happen at time now.
func (lim *Limiter) AllowN(now time.Time, n int64) bool {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	lim.advance(now)

	elapsed := now.Sub(lim.curr.Start())
	weight := float64(lim.size-elapsed) / float64(lim.size)
	count := int64(weight*float64(lim.prev.Count())) + lim.curr.Count()

	if count+n > lim.limit {
		return false
	}

	lim.curr.AddCount(n)
	return true
}

// advance updates the current/previous windows resulting from the passage of time.
func (lim *Limiter) advance(now time.Time) {
	// Calculate the start boundary of the expected current-window.
	newCurrStart := now.Truncate(lim.size)

	diffSize := newCurrStart.Sub(lim.curr.Start()) / lim.size
	if diffSize >= 1 {
		// The current-window is at least one-size behind the expected one.

		newPrevCount := int64(0)
		if diffSize == 1 {
			// The new previous-window will overlap with the old current-window,
			// so it inherits the count.
			newPrevCount = lim.curr.Count()
		}
		lim.prev.Reset(newCurrStart.Add(-lim.size), newPrevCount)

		// The new current-window always has zero count.
		lim.curr.Reset(newCurrStart, 0)
	}
}
