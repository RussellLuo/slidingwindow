package slidingwindow

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type caseArg struct {
	t  time.Time
	n  int64
	ok bool
}

func TestLimiter_LocalWindow_SetLimit(t *testing.T) {
	size := time.Second
	limit := int64(10)

	lim, _ := NewLimiter(size, limit, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	got := lim.Limit()
	if got != limit {
		t.Errorf("lim.Limit() = %d, want: %d", got, limit)
	}

	newLimit := int64(12)
	lim.SetLimit(newLimit)
	got = lim.Limit()
	if got != newLimit {
		t.Errorf("lim.Limit() = %d, want: %d", got, newLimit)
	}
}

func TestLimiter_LocalWindow_AllowN(t *testing.T) {
	size := time.Second
	limit := int64(10)

	lim, _ := NewLimiter(size, limit, func() (Window, StopFunc) {
		return NewLocalWindow()
	})

	d := 100 * time.Millisecond
	t0 := time.Now().Truncate(size)
	t1 := t0.Add(1 * d)
	t2 := t0.Add(2 * d)
	t5 := t0.Add(5 * d)
	t10 := t0.Add(10 * d)
	t12 := t0.Add(12 * d)
	t15 := t0.Add(15 * d)
	t30 := t0.Add(30 * d)

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 2, true},
		{t2, 3, true},
		{t5, 5, false}, // count will be (1 + 2 + 3 + 5) = 11, so it fails

		// prev-window: [t0, t0 + 1s), count: 6
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 2, true},
		{t12, 5, false}, // count will be (4/5*6 + 2 + 5) ≈ 11, so it fails
		{t15, 5, true},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 10, true},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			ok := lim.AllowN(c.t, c.n)
			if ok != c.ok {
				t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
					c.t, c.n, ok, c.ok)
			}
		})
	}
}

type MemDatastore struct {
	data map[string]int64
	mu   sync.RWMutex
}

func newMemDatastore() *MemDatastore {
	return &MemDatastore{
		data: make(map[string]int64),
	}
}

func (d *MemDatastore) fullKey(key string, start int64) string {
	return fmt.Sprintf("%s@%d", key, start)
}

func (d *MemDatastore) Add(key string, start, delta int64) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := d.fullKey(key, start)
	d.data[k] += delta
	return d.data[k], nil
}

func (d *MemDatastore) Get(key string, start int64) (int64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	k := d.fullKey(key, start)
	return d.data[k], nil
}

func TestLimiter_SyncWindow_AllowN(t *testing.T) {
	size := time.Second
	limit := int64(10)

	store := newMemDatastore()
	newWindow := func() (Window, StopFunc) {
		// Sync will happen every 200ms (syncInterval), but for test purpose,
		// we check at the 600ms boundaries (i.e. t6 and t16, see cases below).
		//
		// Because we need to wait for at least twice of syncInterval to ensure
		// that the two parallel limiters have a consistent view of the count
		// after two times of exchange with the central datastore.
		//
		// e.g.:
		//
		//     [Initial]
		//         lim1 (count: 0)                   datastore (count: 0)
		//         lim2 (count: 0)                   datastore (count: 0)
		//
		//     [Allow_1]
		//         lim1 (count: 1)                   datastore (count: 0)
		//         lim2 (count: 1)                   datastore (count: 0)
		//
		//     [1st Sync] -- inconsistent
		//         lim1 (count: 1)  -- add-req  -->  datastore (count: 0)
		//         lim1 (count: 1)  <-- add-resp --  datastore (count: 1)
		//         lim2 (count: 1)  -- add-req  -->  datastore (count: 1)
		//         lim2 (count: 2)  <-- add-resp --  datastore (count: 2)
		//
		//     [2nd Sync] -- consistent
		//         lim1 (count: 1)  -- get-req  -->  datastore (count: 2)
		//         lim1 (count: 2)  <-- get-resp --  datastore (count: 2)
		//         lim2 (count: 2)  -- get-req  -->  datastore (count: 2)
		//         lim2 (count: 2)  <-- get-resp --  datastore (count: 2)
		//
		return NewSyncWindow("test", store, 200*time.Millisecond)
	}

	d := 100 * time.Millisecond
	t0 := time.Now().Truncate(size)
	t1 := t0.Add(1 * d)
	t2 := t0.Add(2 * d)
	t6 := t0.Add(6 * d)
	t10 := t0.Add(10 * d)
	t12 := t0.Add(12 * d)
	t16 := t0.Add(16 * d)
	t18 := t0.Add(18 * d)
	t30 := t0.Add(30 * d)

	cases := []caseArg{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 1, true},
		{t2, 1, true},
		{t6, 5, false}, // after sync: count will be (2*1 + 2*1 + 2*1 + 5) = 11, so it fails

		// prev-window: [t0, t0 + 1s), count: 6
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 2, true},
		{t12, 5, false}, // before sync: count will be (4/5*6 + 2 + 5) ≈ 11, so it fails
		{t16, 5, false}, // after sync: count will be (2/5*6 + 2*2 + 5) ≈ 11, so it fails
		{t18, 5, true},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 10, true},
	}

	parallelisms := []struct {
		name  string
		cases []caseArg
	}{
		{
			"parallel-lim1",
			cases,
		},
		{
			"parallel-lim2",
			cases,
		},
	}
	for _, p := range parallelisms {
		p := p
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			lim, stop := NewLimiter(size, limit, newWindow)

			prevT := t0
			for _, c := range p.cases {
				t.Run("", func(t *testing.T) {
					// Wait for the given duration for Ticker to work.
					time.Sleep(c.t.Sub(prevT))
					prevT = c.t

					ok := lim.AllowN(c.t, c.n)
					if ok != c.ok {
						t.Errorf("lim.AllowN(%v, %v) = %v, want: %v",
							c.t, c.n, ok, c.ok)
					}
				})
			}

			// NOTE: Calling stop() at the top-level of TestLimiter_SyncWindow_AllowN
			// will break. See https://github.com/golang/go/issues/17791 for details.
			stop()
		})
	}
}
