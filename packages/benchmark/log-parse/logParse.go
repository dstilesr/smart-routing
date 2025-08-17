package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"regexp"
)

// A record parsed from the log file.
type logRecord struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
	Time    string `json:"time"`
}

// Summary of processing logs from the workers
type workerLogSummary struct {
	labelMisses int
}

var labelMiss *regexp.Regexp

// Compile the regular expressions
func compileRegex() {
	labelMiss = regexp.MustCompile("LABEL MISS:\\s*<([^<>]+)>")
}

// Iterate over lines of a log file and send log records to the output channel.
func scanLogFile(fp string, out chan<- *logRecord) {
	defer close(out) // Ensure the output channel is closed when done
	f, err := os.Open(fp)
	if err != nil {
		slog.Error("Failed to open log file", "file", fp, "error", err)
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	processed := 0
	for scanner.Scan() {
		l := scanner.Bytes()
		lr := logRecord{}
		jsonErr := json.Unmarshal(l, &lr)
		if jsonErr != nil {
			slog.Error("Failed to parse log line", "line", l, "error", jsonErr)
			continue
		}
		slog.Debug("Processed Record", "level", lr.Level, "message", lr.Message, "time", lr.Time)
		out <- &lr
		processed++
	}
	slog.Info("Completed scanning log file", "file", fp, "records_processed", processed)
}

// Identify if a log has a record of a label miss from a worker
func (lr *logRecord) isLabelMiss() bool {
	if labelMiss == nil {
		slog.Error("Label miss regex is not compiled")
		panic("Label miss regex is not compiled")
	}
	matches := labelMiss.FindStringSubmatch(lr.Message)
	if len(matches) < 2 {
		return false
	}
	return true
}

// Process logs from a worker log file and return a summary
func processWorkerLogs(fp string) *workerLogSummary {
	recs := make(chan *logRecord)
	summary := &workerLogSummary{}
	go scanLogFile(fp, recs)
	for r := range recs {
		if r.isLabelMiss() {
			summary.labelMisses++
		}
	}
	if summary.labelMisses == 0 {
		slog.Warn("No label misses found in worker logs", "file", fp)
	}
	return summary
}

// Print summary of logs analysis
func printSummary(s *workerLogSummary) {
	fmt.Println("Worker Log Summary:")
	fmt.Printf("  Total Label Misses: %d\n", s.labelMisses)
}
