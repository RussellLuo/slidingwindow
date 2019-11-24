package slidingwindow

import (
	"sync"
	"time"
)

// Window represents a fixed-window.
type Window struct {
	// The start boundary of the window.
	// [start, start + size)
	start time.Time

	// The accumulated count of events.
	count int64
}

// Start returns the start boundary.
func (w *Window) Start() time.Time {
	return w.start
}

// Count returns the accumulated count.
func (w *Window) Count() int64 {
	return w.count
}

// SetCount changes the accumulated count to c.
func (w *Window) SetCount(c int64) {
	w.count = c
}

// IncrCount increments the accumulated count by n.
func (w *Window) IncrCount(n int64) {
	w.count += n
}

// Reset sets the state of the window with the given settings.
func (w *Window) Reset(s time.Time, c int64) {
	w.start = s
	w.count = c
}

type Limiter struct {
	size  time.Duration
	limit int64

	mu sync.Mutex
	// The current fixed-window.
	curr Window
	// The previous fixed-window.
	prev Window
}

func NewLimiter(size time.Duration, limit int64) *Limiter {
	return &Limiter{
		size:  size,
		limit: limit,
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

	lim.curr.IncrCount(n)
	return true
}

// advance updates the current/previous windows resulting from the passage of time.
func (lim *Limiter) advance(now time.Time) {
	// Calculate the start boundary of the expected current-window.
	currStart := now.Truncate(lim.size)

	if lim.curr.Start().IsZero() || currStart.Sub(lim.curr.Start()) >= lim.size {
		// The current-window is empty, or its start boundary is at least
		// one-window-size behind.

		// The old current-window becomes the new previous-window.
		lim.prev.Reset(lim.curr.Start(), lim.curr.Count())
		// The new current-window is started from currStart.
		lim.curr.Reset(currStart, 0)
	}
}
