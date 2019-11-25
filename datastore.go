package slidingwindow

import (
	"fmt"
	"strconv"

	"github.com/go-redis/redis"
)

// RedisClient is the interface that both redis.Client and redis.ClusterClient implement.
type RedisClient interface {
	redis.Cmdable
	Close() error
}

type RedisDatastore struct {
	client    RedisClient
	keyPrefix string
}

func NewRedisDatastore(client RedisClient, keyPrefix string) *RedisDatastore {
	return &RedisDatastore{client: client, keyPrefix: keyPrefix}
}

func (d *RedisDatastore) key(start int64) string {
	return fmt.Sprintf("%s@%d", d.keyPrefix, start)
}

func (d *RedisDatastore) IncrBy(start, value int64) (int64, error) {
	key := d.key(start)
	return d.client.IncrBy(key, value).Result()
}

func (d *RedisDatastore) Get(start int64) (n int64, err error) {
	key := d.key(start)
	v, err := d.client.Get(key).Result()
	if err == nil {
		n, err = strconv.ParseInt(v, 10, 64)
	}
	return n, err
}

func (d *RedisDatastore) Del(start int64) (int64, error) {
	key := d.key(start)
	return d.client.Del(key).Result()
}
