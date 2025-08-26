package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

func (wid workerId) getQueue() string {
	return fmt.Sprintf("task-runners:%s:jobs", wid)
}

// Check if the worker is available by checking if it is in the available workers set
func (wid workerId) isAvailable(r *redis.Client, c context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
	defer cancel()

	a, err := r.SIsMember(ctx, availableWorkersKey, string(wid)).Result()
	if err != nil {
		slog.Error("Unable to check worker availability!", "error", err)
		return false, err
	}
	return a, nil
}

func (wid workerId) sendTask(t *taskRequest, r *redis.Client, c context.Context) error {
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
	defer cancel()

	tJson, jsonErr := json.Marshal(t)
	if jsonErr != nil {
		slog.Error("JSON serialization error", "error", jsonErr, "task_id", t.TaskID)
		return jsonErr
	}
	_, err := r.RPush(ctx, wid.getQueue(), tJson).Result()
	if err != nil {
		slog.Error("Unable to send task!", "error", err, "task_id", t.TaskID)
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
		slog.Error("Error receiving task result", "error", err, "task_id", t.TaskID)
		return "", err
	}
	return m.Payload, nil
}

// Determine whether a sorted slice of worker IDs contains a specific worker ID using binary search.
func (wids workerIds) containsSorted(wid workerId) bool {
	var i, j int = 0, len(wids) - 1
	for i <= j {
		curr := (i + j) / 2
		switch {
		case wids[curr] == wid:
			return true
		case wids[curr] < wid:
			i = curr + 1
		case wids[curr] > wid:
			j = curr - 1
		}
	}
	return false
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
