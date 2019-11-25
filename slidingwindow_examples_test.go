package slidingwindow_test

import (
	"fmt"
	"time"

	sw "github.com/RussellLuo/slidingwindow"
	"github.com/go-redis/redis"
)

func Example_localWindow() {
	// import sw "github.com/RussellLuo/slidingwindow"

	lim, stop := sw.NewLimiter(time.Second, 10, func() (sw.Window, sw.StopFunc) {
		return sw.NewLocalWindow()
	})
	defer stop()

	ok := lim.Allow()
	fmt.Printf("ok: %v\n", ok)

	// Output:
	// ok: true
}

func Example_redisSyncWindow() {
	// import sw "github.com/RussellLuo/slidingwindow"

	size := time.Second
	store := sw.NewRedisDatastore(
		redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
		2*size, // twice of window-size is just enough.
	)

	lim, stop := sw.NewLimiter(size, 10, func() (sw.Window, sw.StopFunc) {
		return sw.NewSyncWindow("test", store, time.Second)
	})
	defer stop()

	ok := lim.Allow()
	fmt.Printf("ok: %v\n", ok)

	// Output:
	// ok: true
}
