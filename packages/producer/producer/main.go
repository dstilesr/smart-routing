/*
The producer package's purpose is to simulate traffic by sending requests to run tasks to the
dispatcher and thence to the worker pool.
*/
package main

import (
	"flag"
	"log/slog"
	"os"
	"sync"
)

var numLabels int
var numProducers int
var logsPath string
var settingsPath string
var totalRequests int
var logger *slog.Logger
var waitAvgSeconds float64
var waitStdDevSeconds float64

// Setup Flags to receive values from CLI arguments and start up logger
func startUp() {
	flag.IntVar(&numLabels, "labels", 6, "Number of different labels to use")
	flag.IntVar(&numProducers, "producers", 2, "Number of producers to run concurrently")
	flag.StringVar(&logsPath, "log-file", "producer.log", "Path to the log file")
	flag.StringVar(&settingsPath, "settings", "run-settings.json", "Path to save the settings file for tracking")
	flag.IntVar(&totalRequests, "requests", 200, "Total number of requests to send")
	flag.Float64Var(&waitAvgSeconds, "wait-avg", 1.0, "Average wait time between requests in seconds")
	flag.Float64Var(&waitStdDevSeconds, "wait-stddev", 0.5, "Standard deviation of time between requests in seconds")
	flag.Parse()

	if waitStdDevSeconds <= 0 || waitAvgSeconds <= 0 {
		flag.Usage()
		os.Exit(1)
	}

	if os.Getenv("DISPATCHER_URL") == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	startUp()
	// Logger setup
	logFile, err := os.OpenFile(logsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0655)
	if err != nil {
		slog.Error("Error opening log file", "error", err)
		os.Exit(1)
	}
	defer logFile.Close()
	defer saveSettings()
	logger = slog.New(slog.NewJSONHandler(logFile, nil))

	// Start concurrent producers
	c := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(numProducers)
	for range numProducers {
		prd := newProducer()
		go prd.run(c, &wg)
	}
	for i := range totalRequests {
		c <- i
	}
	close(c)
	wg.Wait()
}
