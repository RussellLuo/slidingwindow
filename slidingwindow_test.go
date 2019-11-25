package slidingwindow

import (
	"testing"
	"time"
)

func TestLimiter_LocalWindow_AllowN(t *testing.T) {
	size := time.Second
	limit := int64(10)

	d := 100 * time.Millisecond
	t0 := time.Now().Truncate(size)
	t1 := t0.Add(1 * d)
	t2 := t0.Add(2 * d)
	t5 := t0.Add(5 * d)
	t10 := t0.Add(10 * d)
	t12 := t0.Add(12 * d)
	t15 := t0.Add(15 * d)
	t30 := t0.Add(30 * d)

	cases := []struct {
		t  time.Time
		n  int64
		ok bool
	}{
		// prev-window: empty, count: 0
		// curr-window: [t0, t0 + 1s), count: 0
		{t0, 1, true},
		{t1, 2, true},
		{t2, 3, true},
		{t5, 5, false}, // count will be 11, so it fails

		// prev-window: [t0, t0 + 1s), count: 6
		// curr-window: [t10, t10 + 1s), count: 0
		{t10, 2, true},
		{t12, 5, false}, // count will be 11, so it fails
		{t15, 5, true},

		// prev-window: [t30 - 1s, t30), count: 0
		// curr-window: [t30, t30 + 1s), count: 0
		{t30, 10, true},
	}

	lim, stop := NewLimiter(size, limit, func() (Window, StopFunc) {
		return NewLocalWindow()
	})
	defer stop()

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
