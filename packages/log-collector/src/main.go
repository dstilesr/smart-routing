// Log Collector Service. This is a simple HTTP server to collect logs from
// different workers into a single file.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
)

type response struct {
	Message string `json:"message"`
}

// Check if parent directory exists, if not, create it
func chekParentDir(path string) {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		slog.Error("Unable to create log directory!", "error", err)
		os.Exit(1)
	}
}

func main() {
	logsPath := os.Getenv("LOG_FILE")
	if logsPath == "" {
		logsPath = "collected-logs.log"
	}
	chekParentDir(logsPath)
	outFile, err := os.OpenFile(logsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0655)
	if err != nil {
		slog.Error("Unable to open log file!", "error", err)
		os.Exit(1)
	}
	defer outFile.Close()
	send := make(chan string, 32)
	out := make(chan writeResult)

	// Start Worker and Monitor
	go handleMessages(send, out, outFile)
	go monitorResults(out)

	http.HandleFunc(
		"/log",
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			message, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Error reading request body", http.StatusBadRequest)
				return
			}

			// Send message to worker
			send <- string(message)
			jsonRsp, jsonErr := json.Marshal(response{Message: "Message received"})
			if jsonErr != nil {
				http.Error(w, "Error creating JSON response", http.StatusInternalServerError)
				slog.Error("Error creating JSON response", "error", jsonErr)
				return
			}

			// Write response
			w.Header().Set("Content-Type", "application/json")
			n, rspErr := w.Write(jsonRsp)
			if rspErr != nil {
				slog.Error("Error writing response", "error", rspErr)
				return
			}
			slog.Info("Response sent", "bytes_written", n)
		},
	)

	// Start HTTP server
	addr := fmt.Sprintf("0.0.0.0:%s", os.Getenv("PORT"))
	slog.Info("Starting server", "address", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
