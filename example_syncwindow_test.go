package slidingwindow_test

import (
	"fmt"
	"strconv"
	"time"

	sw "github.com/RussellLuo/slidingwindow"
	"github.com/go-redis/redis"
)

// RedisDatastore is a reference implementation, which can be used directly
// if you happen to use go-redis.
type RedisDatastore struct {
	client *redis.Client
	ttl    time.Duration
}

func newRedisDatastore(client *redis.Client, ttl time.Duration) *RedisDatastore {
	return &RedisDatastore{client: client, ttl: ttl}
}

func (d *RedisDatastore) fullKey(key string, start int64) string {
	return fmt.Sprintf("%s@%d", key, start)
}

func (d *RedisDatastore) Add(key string, start, value int64) (int64, error) {
	k := d.fullKey(key, start)
	c, err := d.client.IncrBy(k, value).Result()
	if err != nil {
		return 0, err
	}
	// Ignore the possible error from EXPIRE command.
	d.client.Expire(k, d.ttl).Result() // nolint:errcheck
	return c, err
}

func (d *RedisDatastore) Get(key string, start int64) (int64, error) {
	k := d.fullKey(key, start)
	value, err := d.client.Get(k).Result()
	if err != nil {
		if err == redis.Nil {
			// redis.Nil is not an error, it only indicates the key does not exist.
			err = nil
		}
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func Example_syncWindow() {
	size := time.Second
	store := newRedisDatastore(
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
