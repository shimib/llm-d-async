package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ pipeline.DispatchGate = (*RedisQuotaGate)(nil)
var _ pipeline.AttributeGate = (*RedisQuotaGate)(nil)

type QuotaMode string

const (
	QuotaModeRateLimit   QuotaMode = "rate-limit"
	QuotaModeConcurrency QuotaMode = "concurrency"
)

type QuotaStrategy string

const (
	QuotaStrategyBlock    QuotaStrategy = "block"
	QuotaStrategyClassify QuotaStrategy = "classify"
)

type RedisQuotaGate struct {
	rdb         *redis.Client
	attribute   string
	mode        QuotaMode
	strategy    QuotaStrategy
	limit       int
	window      time.Duration
	prefix      string
	overflowObj int
}

func NewRedisQuotaGate(client *redis.Client, attribute string, mode QuotaMode, strategy QuotaStrategy, limit int, window time.Duration, prefix string, overflowObj int) *RedisQuotaGate {
	return &RedisQuotaGate{
		rdb:         client,
		attribute:   attribute,
		mode:        mode,
		strategy:    strategy,
		limit:       limit,
		window:      window,
		prefix:      prefix,
		overflowObj: overflowObj,
	}
}

// Budget implements api.DispatchGate. For quota gates, we return 1.0 (open)
// because the actual gating happens at the message level via Acquire.
func (g *RedisQuotaGate) Budget(ctx context.Context) float64 {
	return 1.0
}

// Acquire implements api.AttributeGate.
func (g *RedisQuotaGate) Acquire(ctx context.Context, attributes map[string]string) (bool, string, func(), error) {
	val, ok := attributes[g.attribute]
	if !ok {
		// If the attribute is missing, we allow it by default (or we could reject it).
		// For now, let's allow it but log a warning.
		return true, "", func() {}, nil
	}

	key := fmt.Sprintf("%s%s:%s", g.prefix, g.attribute, val)

	var allowed bool
	var release func()
	var err error

	switch g.mode {
	case QuotaModeConcurrency:
		allowed, release, err = g.acquireConcurrency(ctx, key)
	case QuotaModeRateLimit:
		allowed, release, err = g.acquireRateLimit(ctx, key)
	default:
		allowed, release, err = true, func() {}, nil
	}

	if err != nil {
		return false, "", nil, err
	}

	var objective string
	if g.strategy == QuotaStrategyClassify {
		if allowed {
			objective = "" // Let queue/topic default apply
		} else {
			objective = strconv.Itoa(g.overflowObj)
			// In classify mode, we always allow the request but tag it as overflow.
			allowed = true
			release = func() {}
		}
	}

	return allowed, objective, release, nil
}

func (g *RedisQuotaGate) acquireConcurrency(ctx context.Context, key string) (bool, func(), error) {
	// Use Lua script for atomic check and increment
	script := `
		local current = redis.call("GET", KEYS[1])
		if current and tonumber(current) >= tonumber(ARGV[1]) then
			return 0
		end
		local new_val = redis.call("INCR", KEYS[1])
		if tonumber(new_val) == 1 then
			redis.call("EXPIRE", KEYS[1], ARGV[2])
		end
		return 1
	`
	// TTL is window size, or a default 5m if window is 0
	ttl := int(g.window.Seconds())
	if ttl <= 0 {
		ttl = 300
	}

	res, err := g.rdb.Eval(ctx, script, []string{key}, g.limit, ttl).Result()
	if err != nil {
		return false, nil, err
	}

	if res.(int64) == 0 {
		return false, nil, nil
	}

	release := func() {
		// Use a background context for release to ensure it runs even if the request context is canceled
		releaseScript := `
			local current = redis.call("GET", KEYS[1])
			if current and tonumber(current) > 0 then
				redis.call("DECR", KEYS[1])
			end
		`
		err := g.rdb.Eval(context.Background(), releaseScript, []string{key}).Err()
		if err != nil {
			log.Log.Error(err, "Failed to release concurrency quota", "key", key)
		}
	}

	return true, release, nil
}

func (g *RedisQuotaGate) acquireRateLimit(ctx context.Context, key string) (bool, func(), error) {
	// Sliding window rate limit using Sorted Set
	now := time.Now().UnixNano()
	windowNano := g.window.Nanoseconds()
	min := now - windowNano

	script := `
		redis.call("ZREMRANGEBYSCORE", KEYS[1], 0, ARGV[1])
		local count = redis.call("ZCARD", KEYS[1])
		if count >= tonumber(ARGV[2]) then
			return 0
		end
		redis.call("ZADD", KEYS[1], ARGV[3], ARGV[3])
		redis.call("EXPIRE", KEYS[1], ARGV[4])
		return 1
	`
	// TTL is window size plus some buffer (e.g., 2x window)
	ttl := int(g.window.Seconds()) * 2
	if ttl <= 0 {
		ttl = 3600
	}

	res, err := g.rdb.Eval(ctx, script, []string{key}, min, g.limit, now, ttl).Result()
	if err != nil {
		return false, nil, err
	}

	if res.(int64) == 0 {
		return false, nil, nil
	}

	return true, func() {}, nil
}
