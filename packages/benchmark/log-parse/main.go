package main

import (
	"flag"
	"os"
	"path/filepath"
	"sync"
)

var logsDir string

func setupFlags() {
	flag.StringVar(
		&logsDir,
		"logs-dir",
		"./logs",
		"Folder with the logs to process. Must have 'collectors.log' and 'producer.log' files and a 'settings.json' file.",
	)
	flag.Parse()
}

func main() {
	setupFlags()
	compileRegex()

	wrkLogs := filepath.Join(logsDir, "collector.log")
	if _, err := os.Stat(wrkLogs); os.IsNotExist(err) {
		panic("Worker log file does not exist: " + wrkLogs)
	}
	prodLogs := filepath.Join(logsDir, "producer.log")
	if _, err := os.Stat(prodLogs); os.IsNotExist(err) {
		panic("Producer log file does not exist: " + prodLogs)
	}

	// Run processors on log files
	wg := sync.WaitGroup{}
	wg.Add(2)
	go processWorkerLogs(wrkLogs, &wg)
	go processProducerLogs(prodLogs, &wg)
	wg.Wait()
}
