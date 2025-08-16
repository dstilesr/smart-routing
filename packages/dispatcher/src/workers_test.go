package main

import (
	"context"
	"encoding/json"
	"testing"

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

// Test worker availability checking functionality.
func TestIsAvailable(t *testing.T) {
	// Setup
	r, c := mockRedis()
	defer r.Close()

	_, err := r.SAdd(c, availableWorkersKey, "worker1").Result()
	if err != nil {
		t.Fatalf("Failed to add worker to available workers: %v", err)
	}
	wid := workerId("worker1")
	wid2 := workerId("worker2")

	a, cErr := wid.isAvailable(r, c)
	if cErr != nil {
		t.Fatalf("Error checking worker availability: %v", cErr)
	} else if !a {
		t.Error("Expected worker1 to be available, but it was not")
	}

	a, cErr = wid2.isAvailable(r, c)
	if cErr != nil {
		t.Fatalf("Error checking worker2 availability: %v", cErr)
	} else if a {
		t.Error("Expected worker2 to be unavailable, but it was available")
	}
}

// Test sending a task to a worker's queue
func TestSendTask(t *testing.T) {
	r, c := mockRedis()
	defer r.Close()

	wid := workerId("worker1")
	q := wid.getQueue()
	tr := taskRequest{
		TaskID:       "test-task",
		Label:        "test-label",
		TaskType:     "test-type",
		Parameters:   "{}",
		ReturnResult: false,
	}
	err := wid.sendTask(&tr, r, c)
	if err != nil {
		t.Fatalf("Failed to send task to worker: %v", err)
	}

	// Check if the task was added to the worker's queue
	l, rErr := r.LLen(c, q).Result()
	if rErr != nil {
		t.Fatalf("Failed to get length of worker queue: %v", rErr)
	}
	if l != 1 {
		t.Errorf("Expected 1 task in worker queue, got %d", l)
	}

	res, rErr := r.LPop(c, q).Result()
	if rErr != nil {
		t.Fatalf("Failed to pop task from worker queue: %v", rErr)
	}
	ntr := taskRequest{}
	if err := json.Unmarshal([]byte(res), &ntr); err != nil {
		t.Error("Failed to unmarshal task from worker queue:", err)
	}
}
