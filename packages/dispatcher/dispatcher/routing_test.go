package main

import (
	"slices"
	"testing"
)

// Test getting available workers with a given label
func TestGetAvailableWorkersWithLabel(t *testing.T) {
	r, c := mockRedis(true)
	defer r.Close()

	ws, err := availableWorkersLabel(r, c, "label-1")
	if err != nil {
		t.Fatalf("Error getting available workers with label: %v", err)
	}
	if len(ws) != 1 {
		t.Errorf("Expected 1 available worker with label 'label-1', got %d", len(ws))
	}
	if ws[0] != "work1" {
		t.Errorf("Expected worker 'work1', got: %s", ws[0])
	}
}

func TestGetAvailableWorkers(t *testing.T) {
	r, c := mockRedis(true)
	defer r.Close()

	ws, err := availableWorkers(r, c)
	if err != nil {
		t.Fatalf("Error getting available workers: %v", err)
	}
	if len(ws) != 2 {
		t.Errorf("Expected 2 available workers, got %d", len(ws))
	}
	if !slices.Contains(ws, "work1") || !slices.Contains(ws, "work2") {
		t.Errorf("Expected workers work1 and work2, got: %v", ws)
	}
}

// Test getting workers with a given label
func TestGetWorkersWithLabel(t *testing.T) {
	r, c := mockRedis(true)
	defer r.Close()

	label := "label-1"
	workers, err := getWorkersWithLabel(label, r, c)
	if err != nil {
		t.Fatalf("Error getting workers with label %s: %v", label, err)
	}
	if len(workers) != 2 {
		t.Errorf("Expected 2 workers with label %s, got %d", label, len(workers))
	}

	if !slices.Contains(workers, "work1") || !slices.Contains(workers, "u-work1") {
		t.Errorf("Expected workers work1 and u-work1 with label %s, got %v", label, workers)
	}
}

// Test selecting a queue when no workers are available
func TestDispatchNoAvailable(t *testing.T) {
	r, c := mockRedis(false)
	defer r.Close()

	tr := taskRequest{
		Label:        "label-1",
		Parameters:   "{}",
		TaskType:     "test-task",
		ReturnResult: false,
		TaskID:       "test-task-1",
	}

	wid, err := selectWorkerQueue(&tr, r, c)
	if err != nil {
		t.Fatalf("Error selecting worker queue: %v", err)
	}
	if wid != "all" {
		t.Error("Expected queue to be all (common queue), got:", wid)
	}
}

// Test selecting an available worker with a specific label
func TestSelectWorkerWithLabel(t *testing.T) {
	r, c := mockRedis(true)
	defer r.Close()

	tr := taskRequest{
		Label:        "label-1",
		Parameters:   "{}",
		TaskType:     "test-task",
		ReturnResult: false,
		TaskID:       "test-task-1",
	}
	wid, err := selectLabeledQueue(&tr, r, c)
	if err != nil {
		t.Fatalf("Error selecting labeled queue: %v", err)
	}
	if wid != "work1" {
		t.Errorf("Expected worker work1, got: %s", wid)
	}
	a, err := wid.isAvailable(r, c)
	if err != nil {
		t.Fatalf("Error checking worker availability: %v", err)
	}
	if !a {
		t.Errorf("Expected worker %s to be available, but it was not", wid)
	}
}

// Test selecting a worker when no workers with the label are available
func TestSelectWorkerWithoutLabel(t *testing.T) {
	r, c := mockRedis(true)
	defer r.Close()

	tr := taskRequest{
		Label:        "label-3",
		Parameters:   "{}",
		TaskType:     "test-task",
		ReturnResult: false,
		TaskID:       "test-task-1",
	}
	wid, err := selectLabeledQueue(&tr, r, c)
	if err != nil {
		t.Fatalf("Error selecting labeled queue: %v", err)
	}
	if wid != "all" {
		t.Errorf("Expectedtask to be sent to common queue, got: %s", wid)
	}
}
