package main

import (
	"fmt"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
)

const (
	protocol        = "tcp"
	defaultHost     = "localhost"
	defaultPort     = "6379"
	defaultPoolSize = 5
)

type RedisConfig struct {
	Host     string // Redis host
	Port     string // Redis port
	PoolSize int    // Number of connections in the redis connection pool (default 5)
}

/*
 * NewRedisPool creates a new pool of redis connections
 */
func NewRedisPool(config RedisConfig) *pool.Pool {
	initConfig(&config)
	log.Printf("Creating Redis pool with config: %v", config)
	p, err := pool.New(protocol, fmt.Sprintf("%s:%s", config.Host, config.Port), config.PoolSize)
	if err != nil {
		// fatal error
		rabbitbeans.FailOnError(err, "Cannot create redis pool")
	}

	return p
}

/*
 * initConfig sets proper defaults and makes sure config is properly set.
 */
func initConfig(config *RedisConfig) {
	if config.Host == "" {
		config.Host = defaultHost
	}
	if config.Port == "" {
		config.Port = defaultPort
	}
	if config.PoolSize < 1 {
		config.PoolSize = defaultPoolSize
	}
}

/*
 * RedisPipe implements a pipe interface that has access to a Redis client connection.
 */
type RedisPipe struct {
	*redis.Client
}

/*
 * Creates a new Pipe that has access to Redis.
 */
func NewRedisPipe(redisPool pool.Pool) rabbitbeans.Pipe {
	client, err := redisPool.Get()
	if err != nil {
		// handle error
		rabbitbeans.FailOnError(err, "Cannot get redis connection from pool")
	}
	return RedisPipe{client}
}

// Process transforms every message passing through the "RedisPipe"
func (rp RedisPipe) Process(in chan interface{}) chan interface{} {
	out := make(chan interface{})
	go func() {
		for i := range in {
			rp.Cmd("SET", "foo", "bar")
			log.Printf("REdis done")
			out <- i
		}
		close(out)
	}()
	return out
}
