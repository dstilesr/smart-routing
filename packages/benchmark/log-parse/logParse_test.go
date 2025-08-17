package main

import (
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	compileRegex()
	m.Run()
}

// Test the 'Label miss' regex matching
func TestLogRecordIsLabelMiss(t *testing.T) {
	lr := &logRecord{
		Level:   "error",
		Message: "LABEL MISS: <label-1>",
		Time:    "2023-10-01T12:00:00Z",
	}

	if !lr.isLabelMiss() {
		t.Errorf("Expected log record to be a label miss, but it was not")
	}

	lr.Message = "Some other message"
	if lr.isLabelMiss() {
		t.Errorf("Expected log record not to be a label miss, but it was")
	}
}

func TestGetTaskRuntime(t *testing.T) {
	lr := &logRecord{
		Level:   "info",
		Message: "Task completed in 4.864089 seconds",
		Time:    "2023-10-01T12:00:00Z",
	}

	runtime := lr.taskRuntime()
	if !(runtime > 4.864088 && runtime < 4.864090) {
		// Allow a small margin of error due to floating point precision
		t.Errorf("Expected task runtime to be 4.864089, got %f", runtime)
	}

	lr.Message = "This is a random message 998.76"
	runtime = lr.taskRuntime()
	if runtime != -1.0 {
		t.Errorf("Expected task runtime to be -1.0, got %f", runtime)
	}
}

// Test that logs can be read and parsed from a sample file
func TestScanLogFile(t *testing.T) {
	fp, err := filepath.Abs(".")
	logFile := filepath.Join(filepath.Dir(fp), "test-assets", "sample-log.jsonl")
	if err != nil {
		panic(err)
	}
	recs := make(chan *logRecord)
	res := make([]*logRecord, 0)
	go scanLogFile(logFile, recs)

	for r := range recs {
		res = append(res, r)
	}
	if len(res) != 4 {
		t.Errorf("Expected 4 records, got %d", len(res))
	}
}
