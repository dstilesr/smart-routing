package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"regexp"
)

// Interface fo results of processing a file that can print a summary.
type logSummary interface {
	printSummary()
}

// Summary of processing logs from the workers
type workerLogSummary struct {
	labelMisses   int
	totalRuntime  float64
	totalComplete int
}

var labelMiss *regexp.Regexp
var taskCompleted *regexp.Regexp

// Compile the regular expressions
func compileRegex() {
	labelMiss = regexp.MustCompile("LABEL MISS:\\s*<([^<>]+)>")
	taskCompleted = regexp.MustCompile("Task completed in\\s+(\\d+\\.?\\d*)\\s+seconds")
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

// Process logs from a worker log file and return a summary
func processWorkerLogs(fp string) *workerLogSummary {
	recs := make(chan *logRecord)
	summary := &workerLogSummary{}
	go scanLogFile(fp, recs)
	for r := range recs {
		if r.isLabelMiss() {
			summary.labelMisses++
		} else if r.taskRuntime() > 0.0 {
			summary.totalComplete++
			summary.totalRuntime += r.taskRuntime()
		}
	}
	if summary.labelMisses == 0 {
		slog.Warn("No label misses found in worker logs", "file", fp)
	}
	return summary
}

// Print summary of logs analysis
func (s *workerLogSummary) printSummary() {
	fmt.Println("Worker Log Summary:")
	fmt.Printf("  Total Label Misses: %d\n", s.labelMisses)
	fmt.Printf("  Tasks Completed: %d\n", s.totalComplete)
	if s.totalComplete > 0 {
		avgRuntime := s.totalRuntime / float64(s.totalComplete)
		fmt.Printf("  Average Task Runtime: %.4f seconds\n", avgRuntime)
	}
}
