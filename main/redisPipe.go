package main

import (
	"fmt"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/urjitbhatia/rabbitbeans"
	"log"
)

const (
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
 * A Pipe of type redis - intercepts messages and interacts with redis
 */
type RedisPipe struct {
	conn *redis.Client
}

/*
 * Creates a new Pipe that has access to Redis.
 */
func NewRedisPipe(config RedisConfig) (*RedisPipe, error) {
	initConfig(&config)
	log.Printf("Config redis: %v", config)
	p, err := pool.New("tcp", fmt.Sprintf("%s:%s", config.Host, config.Port), config.PoolSize)
	if err != nil {
		// handle error
		rabbitbeans.FailOnError(err, "Cannot create redis pool")
	}

	// In another go-routine
	conn, err := p.Get()
	if err != nil {
		// handle error
		rabbitbeans.FailOnError(err, "Cannot connecto to redis")
	}
	redisPipe := &RedisPipe{
		conn,
	}
	return redisPipe, nil
}

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

func (rp *RedisPipe) Process(in chan interface{}) chan interface{} {
	out := make(chan interface{})
	go func() {
		for i := range in {
			log.Printf("Got int: %d", i)
			out <- i
		}
		close(out)
	}()
	return out
}
