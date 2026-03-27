package redis

import (
	"flag"
	"os"
)

var (
	RedisAddr     = flag.String("redis.addr", "localhost:6379", "address of the Redis server")
	RedisUser     = flag.String("redis.user", os.Getenv("REDIS_USERNAME"), "username for the Redis server")
	RedisPassword = flag.String("redis.password", os.Getenv("REDIS_PASSWORD"), "password for the Redis server")
)
