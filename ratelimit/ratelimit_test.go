package ratelimit_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/dreamsofcode-io/testcontainers/ratelimit"
)

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()

	// 🐳 Start Redis container
	redisContainer, err := tcredis.Run(ctx,
		"redis:7-alpine", // ✅ valid tag
		tcredis.WithSnapshotting(10, 1),
		tcredis.WithLogLevel(tcredis.LogLevelVerbose),
	)
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}

	defer func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %v", err)
		}
	}()

	// 🧠 Get connection string for go-redis
	redisURI, err := redisContainer.ConnectionString(ctx)
	assert.NoError(t, err)

	// 🔌 Connect client
	opts, err := redis.ParseURL(redisURI)
	assert.NoError(t, err)

	client := redis.NewClient(opts)
	defer client.Close()

	// ✅ Initialize your limiter (3 requests per minute)
	limiter := ratelimit.New(client, 3, time.Minute)

	ip := "192.168.1.54"

	t.Run("happy path flow", func(t *testing.T) {
		res, err := limiter.AddAndCheckIfExceeds(ctx, net.ParseIP(ip))
		assert.NoError(t, err)

		// Rate should not be exceeded
		assert.False(t, res.IsExceeded())

		// Check key value incremented
		val, err := client.Get(ctx, ip).Result()
		assert.NoError(t, err)
		assert.Equal(t, "1", val)

		client.FlushAll(ctx)
	})

	t.Run("should expire after three times", func(t *testing.T) {
		client.Set(ctx, ip, "3", 0)

		res, err := limiter.AddAndCheckIfExceeds(ctx, net.ParseIP(ip))
		assert.NoError(t, err)

		// Rate should be exceeded
		assert.True(t, res.IsExceeded())

		// Check TTL (time-to-live) is set
		ttl, err := client.TTL(ctx, ip).Result()
		assert.NoError(t, err)
		assert.Greater(t, ttl, time.Duration(0))
	})
}
