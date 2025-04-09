package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"

	"github.com/hibiken/asynq"
)

const redisAddr = "localhost:6379"

type JobPayload struct {
	Message  string `json:"message"`
	Priority string `json:"priority"`
}

func processJob(ctx context.Context, t *asynq.Task) error {
	var payload JobPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return errors.New("failed to parse job payload")
	}

	log.Printf("🛠️ Processing Job: %s (Priority: %s)\n", payload.Message, payload.Priority)

	// // Simulate failure (every job fails with 100% chance)
	// log.Println("❌ Job failed, retrying...")
	// return errors.New("forced failure to test dead queue")

	// Simulate job failure (20% chance)
	if rand.Intn(3) != 0 {
		log.Println("❌ Job failed, retrying...")
		return errors.New("simulated job failure")
	}

	log.Println("✅ Job Completed")
	return nil
}

func main() {

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{
			Queues: map[string]int{
				"high":   3,
				"medium": 2,
				"low":    1,
			},
			Concurrency:    5,
			RetryDelayFunc: asynq.DefaultRetryDelayFunc, // ✅ Corrected retry function
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				log.Printf("❌ Job failed permanently: %v\n", err)
			}),
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("ProcessJob", processJob)

	log.Println("🚀 Starting Asynq Worker with Priority and Retries")
	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
