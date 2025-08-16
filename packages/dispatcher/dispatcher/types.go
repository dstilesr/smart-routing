package main

// String alias to represent a running worker's ID.
type workerId string

// A request to run a task on a worker.
type taskRequest struct {
	TaskID       string `json:"task_id"`
	TaskType     string `json:"task_type"`
	Label        string `json:"label"`
	Parameters   string `json:"parameters_json"`
	ReturnResult bool   `json:"return_result"`
}
