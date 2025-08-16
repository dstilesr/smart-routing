package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Get the IDs for workers that are currently available
func getAvailableWorkerIds(r *redis.Client, c context.Context) ([]workerId, error) {
	ctx, cancel := context.WithTimeout(c, 1*time.Second)
	defer cancel()
	m, err := r.SMembers(ctx, availableWorkersKey).Result()
	if err != nil {
		slog.Error("Unable to get available workers!", "error", err)
		return []workerId{}, err
	}
	return stringToWidSlice(m), nil
}

// Get the IDs for all currently running workers
func getRunningWorkerIds(r *redis.Client, c context.Context) ([]workerId, error) {
	ctx, cancel := context.WithTimeout(c, 1*time.Second)
	defer cancel()
	m, err := r.SMembers(ctx, runningWorkerskey).Result()
	if err != nil {
		slog.Error("Unable to get running workers!", "error", err)
		return []workerId{}, err
	}
	return stringToWidSlice(m), nil
}

// Select a worker to process the given task request.
func selectWorkerQueue(t *taskRequest, r *redis.Client, c context.Context, random bool) (workerId, error) {
	if random {
		// Send to common queue
		return workerId("all"), nil
	}
	return selectLabeledQueue(t, r, c)
}

// Select a worker based on the task label and worker labels
func selectLabeledQueue(t *taskRequest, r *redis.Client, c context.Context) (workerId, error) {
	// TODO: Implement this function
	return workerId("all"), nil
}
