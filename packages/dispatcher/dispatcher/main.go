package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

// flags
var randomDispatch bool

// Setup CLI flags
func flagsSetup() {
	flag.BoolVar(&randomDispatch, "random-dispatch", false, "Use random dispatching instead of 'smart' dispatching")
}

func main() {
	// flags init
	flagsSetup()
	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
	})

	http.HandleFunc("/health", healthCheckAPI)
	http.HandleFunc(
		"/workers",
		func(w http.ResponseWriter, r *http.Request) {
			runningWorkersAPI(w, r, client)
		})
	http.HandleFunc(
		"/send-task",
		func(w http.ResponseWriter, r *http.Request) {
			dispatchTaskAPI(w, r, client)
		})
	http.HandleFunc(
		"/run-task",
		func(w http.ResponseWriter, r *http.Request) {
			runTaskAPI(w, r, client)
		})
	log.Fatal(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", os.Getenv("PORT")), nil))
}
