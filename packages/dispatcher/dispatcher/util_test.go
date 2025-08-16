package main

import (
	"context"
	"fmt"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// mockRedis creates a mock Redis server for testing purposes. Returns a Redis client
// and a background context. If setup is true, it will also set up some mock data in Redis.
func mockRedis(setup bool) (*redis.Client, context.Context) {
	mr, err := miniredis.Run()
	if err != nil {
		panic("Could not start mock Redis server: " + err.Error())
	}
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := context.Background()

	if setup {
		setupTestData(r, c)
	}
	return r, c
}

// Set up some mock data on Redis for testing purposes. Adds 4 workers: work1, u-work1, work2, and u-work2.
// Those with the u-prefix are unavailable workers. The workers will have a single label-1 or label-2,
// corresponding to the worker number.
func setupTestData(r *redis.Client, c context.Context) {
	// Add running workers
	_, err := r.SAdd(c, runningWorkerskey, "work1", "work2", "u-work1", "u-work2").Result()
	if err != nil {
		panic(err)
	}

	// Available workers
	_, err = r.SAdd(c, availableWorkersKey, "work1", "work2").Result()
	if err != nil {
		panic(err)
	}

	// Label1
	key := fmt.Sprintf("task-runners:labels:%s:workers", "label-1")
	_, err = r.SAdd(c, key, "work1", "u-work1").Result()
	if err != nil {
		panic(err)
	}

	// Label2
	key = fmt.Sprintf("task-runners:labels:%s:workers", "label-2")
	_, err = r.SAdd(c, key, "work2", "u-work2").Result()
	if err != nil {
		panic(err)
	}
}
