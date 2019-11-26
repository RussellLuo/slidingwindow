package slidingwindow

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

// RedisClient is the interface that both redis.Client and redis.ClusterClient implement.
type RedisClient interface {
	redis.Cmdable
	Close() error
}

type RedisDatastore struct {
	client RedisClient
	ttl    time.Duration
}

func NewRedisDatastore(client RedisClient, ttl time.Duration) *RedisDatastore {
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
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}
