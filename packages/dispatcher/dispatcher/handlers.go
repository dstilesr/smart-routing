package main

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/redis/go-redis/v9"
)

type SimpleResponse struct {
	Message string `json:"message"`
}

func getResponseJSON(m string) []byte {
	b, err := json.Marshal(&SimpleResponse{Message: m})
	if err != nil {
		panic("Error serializing response: " + err.Error())
	}
	return b
}

// Parse a task request from the HTTP request body.
func taskFromRequest(r *http.Request) (*taskRequest, error) {
	var t taskRequest
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		slog.Error("Error decoding request body", "error", err)
		return nil, err
	}
	return &t, nil
}

// API method to check the health of the service
func healthCheckAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Respond with a simple message
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write(getResponseJSON("OK"))
	if err != nil {
		slog.Error("Error writing response", "error", err)
	}
}

// API method to get the list of running workers
func runningWorkersAPI(w http.ResponseWriter, r *http.Request, rd *redis.Client) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	out := map[string][]string{}
	ws, err := getRunningWorkerIds(rd, r.Context())
	if err != nil {
		http.Error(w, "Error retrieving running workers", http.StatusInternalServerError)
		slog.Error("Error retrieving running workers", "error", err)
		return
	}
	out["workers"] = widToStringSlice(ws)
	jsonOut, jsonErr := json.Marshal(out)
	if jsonErr != nil {
		panic("Error serializing response: " + jsonErr.Error())
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	_, writeErr := w.Write(jsonOut)
	if writeErr != nil {
		slog.Error("Error writing response", "error", writeErr)
	}
}

// API method to dispatch a task to a worker
func dispatchTaskAPI(w http.ResponseWriter, r *http.Request, rd *redis.Client) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	t, err := taskFromRequest(r)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	wid, selectErr := selectWorkerQueue(t, rd, r.Context())

	if selectErr != nil {
		http.Error(w, "Error selecting worker", http.StatusInternalServerError)
		slog.Error("Error selecting worker", "error", selectErr)
		return
	}

	if sendErr := wid.sendTask(t, rd, r.Context()); sendErr != nil {
		http.Error(w, "Error sending task to worker", http.StatusInternalServerError)
		slog.Error("Error sending task to worker", "error", sendErr)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Header().Set("Content-Type", "application/json")
	_, writeErr := w.Write(getResponseJSON("Task dispatched successfully"))
	if writeErr != nil {
		slog.Error("Error writing response", "error", writeErr)
	}
	slog.Info("Sent task to worker", "worker_id", wid, "task_id", t.TaskID)
}

func runTaskAPI(w http.ResponseWriter, r *http.Request, rd *redis.Client) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	t, err := taskFromRequest(r)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if !t.ReturnResult {
		http.Error(
			w,
			"Return result must be true to run the task with this method!",
			http.StatusUnprocessableEntity,
		)
		return
	}

	wid, selectErr := selectWorkerQueue(t, rd, r.Context())

	if selectErr != nil {
		http.Error(w, "Error selecting worker", http.StatusInternalServerError)
		slog.Error("Error selecting worker", "error", selectErr)
		return
	}

	result, err := wid.runTask(t, rd, r.Context())
	if err != nil {
		http.Error(w, "Eror when running task", http.StatusInternalServerError)
		slog.Error("Error when running task", "error", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, writeErr := w.Write([]byte(result))
	if writeErr != nil {
		slog.Error("Error writing response", "error", writeErr)
	}
}
