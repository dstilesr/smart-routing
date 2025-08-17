package main

import (
	"flag"
	"os"
	"path/filepath"
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

	wrkSummary := processWorkerLogs(wrkLogs)
	wrkSummary.printSummary()

	prodSummary := processProducerLogs(prodLogs)
	prodSummary.printSummary()
}
