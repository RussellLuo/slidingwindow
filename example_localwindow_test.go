package slidingwindow_test

import (
	"fmt"
	"time"

	sw "github.com/RussellLuo/slidingwindow"
)

func Example_localWindow() {
	lim, _ := sw.NewLimiter(time.Second, 10, func() (sw.Window, sw.StopFunc) {
		// NewLocalWindow returns an empty stop function, so it's
		// unnecessary to call it later.
		return sw.NewLocalWindow()
	})

	ok := lim.Allow()
	fmt.Printf("ok: %v\n", ok)

	// Output:
	// ok: true
}
