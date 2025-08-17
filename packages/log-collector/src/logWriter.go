package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type writeResult struct {
	Message string
	Success bool
}

// Worker function
func handleMessages(in <-chan string, out chan<- writeResult, f *os.File) {
	for msg := range in {
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		n, err := f.WriteString(msg)
		if err != nil {
			out <- writeResult{
				fmt.Sprintf("Error writing to file: %v", err),
				false,
			}
			continue
		}
		out <- writeResult{fmt.Sprintf("Written %d bytes to file", n), true}
	}
}

// Monitor function
func monitorResults(out <-chan writeResult) {
	for result := range out {
		if result.Success {
			slog.Info("Successfully wrote log message")
		} else {
			slog.Error("Failed to write log message", "error", result.Message)
		}
	}
}
