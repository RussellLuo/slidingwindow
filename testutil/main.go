package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	sw "github.com/RussellLuo/slidingwindow"
	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	size         time.Duration
	limit        int64
	resourceName string
	syncInterval time.Duration
	scale        int
	redisAddr    string
	listenAddr   string

	requestAllowed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "slidingwindow",
			Name:      "requests_total",
			Help:      "count of requests, partitioned by resource, limiter and allow result.",
		},
		[]string{"resource", "limiter", "allow"},
	)
)

type RedisDatastore struct {
	client redis.Cmdable
	ttl    time.Duration
}

func NewRedisDatastore(client redis.Cmdable, ttl time.Duration) *RedisDatastore {
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

type Limiter struct {
	name string
	lim  *sw.Limiter
	stop sw.StopFunc
}

func parseFlags() {
	var (
		sizeFlag     = flag.String("size", "1s", "The time duration during which limit takes effect.")
		limitFlag    = flag.Int64("limit", 20, "The maximum events permitted.")
		resourceFlag = flag.String("resource", "test", "The name of the resource that will be limited.")
		syncFlag     = flag.String("sync", "200ms", "The time duration of sync interval.")
		scaleFlag    = flag.Int("scale", 2, "The number of limiters that will work concurrently.")
		redisFlag    = flag.String("redis", "localhost:6379", "The address of the Redis server.")
		listenFlag   = flag.String("listen", "", "The listen address of the HTTP server.")
	)

	flag.Parse()

	var err error
	size, err = time.ParseDuration(*sizeFlag)
	if err != nil {
		panic(err)
	}

	syncInterval, err = time.ParseDuration(*syncFlag)
	if err != nil {
		panic(err)
	}

	limit = *limitFlag
	resourceName = *resourceFlag
	scale = *scaleFlag
	redisAddr = *redisFlag

	listenAddr = *listenFlag
	if listenAddr == "" {
		fmt.Println(`Flag "-listen" is required`)
		os.Exit(1)
	}
}

func newLimiters() (limiters []Limiter) {
	store := NewRedisDatastore(
		redis.NewClient(&redis.Options{
			Addr: redisAddr,
		}),
		2*size, // twice of size is just enough.
	)

	for i := 0; i < scale; i++ {
		lim, stop := sw.NewLimiter(size, limit, func() (sw.Window, sw.StopFunc) {
			return sw.NewSyncWindow(resourceName, sw.NewBlockingSynchronizer(store, syncInterval))
		})
		limiters = append(limiters, Limiter{
			name: fmt.Sprintf("lim-%d", i),
			lim:  lim,
			stop: stop,
		})
	}

	return
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	parseFlags()

	limiters := newLimiters()
	defer func() {
		for _, limiter := range limiters {
			limiter.stop()
		}
	}()

	prometheus.MustRegister(requestAllowed)
	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/allow", func(w http.ResponseWriter, r *http.Request) {
		randI := rand.Intn(scale)
		ok := limiters[randI].lim.Allow()
		fmt.Fprintf(w, "%s: %v", limiters[randI].name, ok)

		requestAllowed.WithLabelValues(resourceName, limiters[randI].name, strconv.FormatBool(ok)).Inc()
	})
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
