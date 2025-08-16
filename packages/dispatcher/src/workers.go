package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func (wid workerId) getQueue() string {
	return fmt.Sprintf("task-runners:%s:jobs", wid)
}

// Check if a worker is available and if so, send the worker Id to the output channel.
func (wid workerId) isAvailableAsync(r *redis.Client, c context.Context, out chan<- workerId, wg *sync.WaitGroup) {
	defer wg.Done()
	a, err := wid.isAvailable(r, c)
	if err != nil {
		slog.Error("Could not check worker availability", "error", err, "workerId", wid)
		return
	} else if a {
		out <- wid
	}
}

// Check if the worker is available by checking if it is in the available workers set
func (wid workerId) isAvailable(r *redis.Client, c context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(c, 500*time.Millisecond)
	defer cancel()

	a, err := r.SIsMember(ctx, availableWorkersKey, string(wid)).Result()
	if err != nil {
		slog.Error("Unable to check worker availability!", "error", err)
		return false, err
	}
	return a, nil
}

func (wid workerId) sendTask(t *taskRequest, r *redis.Client, c context.Context) error {
	ctx, cancel := context.WithTimeout(c, 500*time.Millisecond)
	defer cancel()

	tJson, jsonErr := json.Marshal(t)
	if jsonErr != nil {
		slog.Error("JSOn serialization error", "error", jsonErr)
		return jsonErr
	}
	_, err := r.RPush(ctx, wid.getQueue(), tJson).Result()
	if err != nil {
		slog.Error("Unable to send task!", "error", err)
		return err
	}
	return nil
}

// Run a task until completion or timeout, and return the result
func (wid workerId) runTask(t *taskRequest, r *redis.Client, c context.Context) (string, error) {
	err := wid.sendTask(t, r, c)
	if err != nil {
		return "", err
	}

	// Wait for task result
	key := fmt.Sprintf("task-runners:results:%s", t.TaskID)
	pubsub := r.Subscribe(c, key)
	defer pubsub.Close()

	ctx, cancel := context.WithTimeout(c, taskTimeoutSeconds*time.Second)
	defer cancel()

	m, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		return "", err
	}
	return m.Payload, nil
}

func stringToWidSlice(s []string) []workerId {
	out := make([]workerId, len(s))
	for i, e := range s {
		out[i] = workerId(e)
	}
	return out
}

func widToStringSlice(s []workerId) []string {
	out := make([]string, len(s))
	for i, e := range s {
		out[i] = string(e)
	}
	return out
}
