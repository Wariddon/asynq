package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const redisAddr = "localhost:6379"

// Redis client using go-redis v9
var redisClient = redis.NewClient(&redis.Options{
	Addr: "localhost:6379", // Redis server
})

type LineNotificationPayload struct {
	CorrelationID string `json:"correlation_id"`
	Content       string `json:"content"`
	Priority      string `json:"priority"`
	RetryLimit    int    `json:"retry_limit"`
}

func processJob(ctx context.Context, t *asynq.Task) error {
	var payload LineNotificationPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return errors.New("failed to parse job payload")
	}
	log.Printf("————————————————————————————————————————————————\n")
	log.Printf("🛠️  Processing id: %s,Content: %s (Priority: %s)\n", payload.CorrelationID, payload.Content, payload.Priority)

	// Simulate failure (every job fails with 100% chance)
	log.Println("❌ Job failed, retrying...")
	return errors.New("forced failure to test dead queue")

	//Simulate job failure (20% chance)
	// if rand.Intn(3) != 0 {
	// 	log.Println("❌ Job failed, retrying...")
	// 	return errors.New("simulated job failure")
	// }

	log.Println("✅ Job Completed")
	return nil
}

func main() {

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{
			Queues: map[string]int{
				"critical": 4,
				"high":     3,
				"medium":   2,
				"low":      1,
			},
			Concurrency: 3,

			//RetryDelayFunc: asynq.DefaultRetryDelayFunc, // ✅ Corrected retry function
			RetryDelayFunc: func(no int, e error, task *asynq.Task) time.Duration {

				return time.Duration(no) * 1 * time.Second // Exponential-style backoff
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {

				countRetry, _ := asynq.GetRetryCount(ctx)
				maxRetry, _ := asynq.GetMaxRetry(ctx)

				if maxRetry == countRetry {
					// This runs only after final retry failed — move to DLQ
					log.Printf("❌ FINAL RETRY failed and moved to DLQ: retry count=%v | Error=%v", countRetry, err)

					entry := map[string]interface{}{
						"type":      task.Type(),
						"payload":   json.RawMessage(task.Payload()),
						"error":     err.Error(),
						"timestamp": time.Now().Format(time.RFC3339),
					}

					data, _ := json.Marshal(entry)
					if pushErr := redisClient.LPush(ctx, "custom-dlq:line-failures", data).Err(); pushErr != nil {
						log.Printf("⚠️ Failed to push DLQ entry: %v", pushErr)
					} else {
						log.Println("📦 DLQ entry saved to Redis key: custom-dlq:line-failures")
					}
				}
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
