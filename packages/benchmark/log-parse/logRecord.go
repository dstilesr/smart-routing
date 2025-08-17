package main

import (
	"log/slog"
	"regexp"
	"strconv"
)

// A record parsed from the log file.
type logRecord struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
	Time    string `json:"time"`
}

var labelMiss *regexp.Regexp
var taskCompleted *regexp.Regexp
var requestCompleted *regexp.Regexp

// Compile the regular expressions
func compileRegex() {
	labelMiss = regexp.MustCompile("LABEL MISS:\\s*<([^<>]+)>")
	taskCompleted = regexp.MustCompile("Task completed in\\s+(\\d+\\.?\\d*)\\s+seconds")
	requestCompleted = regexp.MustCompile("Request completed \\[(\\d+)]\\[([\\w\\-/]+)]")
}

// Extract the task runtime if the message indicates a task completion
// Return -1.0 if the task runtime is not in the message
func (lr *logRecord) taskRuntime() float64 {
	match := taskCompleted.FindStringSubmatch(lr.Message)
	if len(match) < 2 {
		return -1.0
	}
	runtime, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		slog.Error("Failed to parse task runtime", "message", lr.Message, "error", err)
		return -1.0
	}
	return runtime
}

// Identify if a log has a record of a label miss from a worker
func (lr *logRecord) isLabelMiss() bool {
	if labelMiss == nil {
		slog.Error("Label miss regex is not compiled")
		panic("Label miss regex is not compiled")
	}
	matches := labelMiss.FindStringSubmatch(lr.Message)
	return len(matches) >= 2
}

// Get the result of a request from a producer log record return the status code of the response
// and the endpoint that was called. Returns -1 if the message does not correspond to a request result.
func (lr *logRecord) requestResult() (int, string) {
	matches := requestCompleted.FindStringSubmatch(lr.Message)
	if len(matches) < 2 {
		return -1, ""
	}
	statusCode, err := strconv.Atoi(matches[1])
	if err != nil {
		slog.Error("Failed to parse request status code", "message", lr.Message, "error", err)
		return -1, ""
	}
	return statusCode, matches[2]
}
