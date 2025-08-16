package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
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
	log.Fatal(http.ListenAndServe(":8000", nil))
}
