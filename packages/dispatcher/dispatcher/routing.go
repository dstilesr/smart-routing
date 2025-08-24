package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// Get the IDs for workers that are currently available with the given label
func availableWorkersLabel(r *redis.Client, c context.Context, l string) ([]workerId, error) {
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
	defer cancel()
	lk := fmt.Sprintf("task-runners:labels:%s:workers", l)
	m, err := r.SInter(ctx, availableWorkersKey, lk).Result()
	if err != nil {
		slog.Error("Unable to get available workers!", "error", err)
		return []workerId{}, err
	}
	return stringToWidSlice(m), nil
}

// Get all available worker IDs
func availableWorkers(r *redis.Client, c context.Context) ([]workerId, error) {
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
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
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
	defer cancel()
	m, err := r.SMembers(ctx, runningWorkerskey).Result()
	if err != nil {
		slog.Error("Unable to get running workers!", "error", err)
		return []workerId{}, err
	}
	return stringToWidSlice(m), nil
}

// Select a worker to process the given task request.
func selectWorkerQueue(t *taskRequest, r *redis.Client, c context.Context) (workerId, error) {
	if randomDispatch {
		// Send to common queue
		return workerId("all"), nil
	}
	return selectLabeledQueue(t, r, c)
}

// Select a worker based on the task label and worker labels
func selectLabeledQueue(t *taskRequest, r *redis.Client, c context.Context) (workerId, error) {
	available, err := availableWorkersLabel(r, c, t.Label)
	if err != nil {
		slog.Error("Error getting available workers", "error", err, "label", t.Label)
		return "", err
	}
	if len(available) > 0 {
		return available[rand.Intn(len(available))], nil
	}
	slog.Warn("No available workers found with label", "label", t.Label, "task_id", t.TaskID)
	return workerId("all"), nil
}

// Get the list of workers that have a specific label
func getWorkersWithLabel(label string, r *redis.Client, c context.Context) ([]workerId, error) {
	key := fmt.Sprintf("task-runners:labels:%s:workers", label)
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
	defer cancel()

	m, err := r.SMembers(ctx, key).Result()
	if err != nil {
		slog.Error("Unable to get workers with label!", "error", err, "label", label)
		return []workerId{}, err
	}
	return stringToWidSlice(m), nil
}

// Get the list of workers that can take on an additional label
func workersWithLabelCapacity(r *redis.Client, c context.Context) ([]workerId, error) {
	ctx, cancel := context.WithTimeout(c, opTimeoutMilliseconds*time.Millisecond)
	defer cancel()

	opts := redis.ZRangeBy{
		Min:    "0",
		Max:    fmt.Sprintf("%d", maxLabelsPerWorker-1),
		Offset: 0,
		Count:  20,
	}
	m, err := r.ZRangeByScore(ctx, workersLabelCountKey, &opts).Result()
	if err != nil {
		slog.Error("Unable to get workers with label capacity!", "error", err)
		return []workerId{}, err
	}
	return stringToWidSlice(m), nil
}
