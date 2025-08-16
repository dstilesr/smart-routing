package main

import (
	"context"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// mockRedis creates a mock Redis server for testing purposes. Returns a Redis client
// and a background context.
func mockRedis() (*redis.Client, context.Context) {
	mr, err := miniredis.Run()
	if err != nil {
		panic("Could not start mock Redis server: " + err.Error())
	}
	return redis.NewClient(&redis.Options{Addr: mr.Addr()}), context.Background()
}
