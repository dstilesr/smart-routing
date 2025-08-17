package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

type produceSettings struct {
	NumLabels         int     `json:"num_labels"`
	NumProducers      int     `json:"num_producers"`
	LogsPath          string  `json:"logs_path"`
	TotalRequests     int     `json:"total_requests"`
	WaitAvgSeconds    float64 `json:"wait_avg_seconds"`
	WaitStdDevSeconds float64 `json:"wait_std_dev_seconds"`
}

type producer struct {
	rng *rand.Rand
}

type taskRequest struct {
	TaskID       string `json:"task_id"`
	TaskType     string `json:"task_type"`
	Label        string `json:"label"`
	Parameters   string `json:"parameters_json"`
	ReturnResult bool   `json:"return_result"`
}

// Initialize a new producer with a random number generator and wait parameters
func newProducer() *producer {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &producer{
		rng: rng,
	}
}

// Create a task request with some random parameters
func (p *producer) createTaskRequest() *taskRequest {
	taskId := fmt.Sprintf("%d-%d", p.rng.Intn(1000000), p.rng.Intn(1000000))

	tr := taskRequest{
		TaskID:       taskId,
		ReturnResult: p.rng.Intn(2) == 0,
		Label:        fmt.Sprintf("label-%d", p.rng.Intn(numLabels)),
		Parameters:   "{}",
		TaskType:     fmt.Sprintf("sample_task_%d", p.rng.Intn(2)+1),
	}

	return &tr
}

// Run a request to the dispatcher
func (p *producer) runRequest() {
	tr := p.createTaskRequest()
	var ep string
	if tr.ReturnResult {
		ep = "run-task"
	} else {
		ep = "send-task"
	}
	url := fmt.Sprintf("%s/%s", os.Getenv("DISPATCHER_URL"), ep)
	jsonBody, jsonErr := json.Marshal(tr)
	if jsonErr != nil {
		logger.Error("Error marshalling task request", "error", jsonErr, "task_id", tr.TaskID)
		return
	}

	rsp, err := http.Post(url, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		logger.Error("Error sending request to dispatcher", "error", err, "task_id", tr.TaskID)
		return
	}
	defer rsp.Body.Close()
	c, readErr := io.ReadAll(rsp.Body)
	if readErr != nil {
		logger.Error("Error reading response from dispatcher", "error", readErr, "task_id", tr.TaskID)
		return
	}
	logger.Info(
		"Request completed",
		"status", rsp.StatusCode,
		"response", string(c),
		"task_id", tr.TaskID,
		"endpoint", ep,
	)
}

// Run a producer to send requests to the dispatcher
// It will kick off a request every time it receives a signal.
func (p *producer) run(sig <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for range sig {
		p.runRequest()
		waitTime := (p.rng.NormFloat64()*waitStdDevSeconds + waitAvgSeconds) * 1000
		waitMilliSeconds := int64(waitTime)
		if waitMilliSeconds < minWaitMilliseconds {
			waitMilliSeconds = minWaitMilliseconds
		}
		time.Sleep(time.Duration(waitMilliSeconds) * time.Millisecond)
	}
}

// Save run settings to JSON for later reference
func saveSettings() {
	settings := produceSettings{
		NumLabels:         numLabels,
		NumProducers:      numProducers,
		LogsPath:          logsPath,
		TotalRequests:     totalRequests,
		WaitAvgSeconds:    waitAvgSeconds,
		WaitStdDevSeconds: waitStdDevSeconds,
	}

	file, err := os.Create(settingsPath)
	if err != nil {
		logger.Error("Error creating settings file", "error", err)
		return
	}
	defer file.Close()

	err = json.NewEncoder(file).Encode(&settings)
	if err != nil {
		logger.Error("Error marshalling settings to JSON", "error", err)
		return
	}
	logger.Info("Settings saved successfully", "path", settingsPath)
}
