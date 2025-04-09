package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaTopic = "asynq_jobs"
	redisAddr  = "localhost:6379"
)

var kafkaBrokers = []string{
	"localhost:9092",
	// "connect-kafka-dev01.devcloud.scb:19092",
	// "connect-kafka-dev02.devcloud.scb:19092",
	// "connect-kafka-dev03.devcloud.scb:19092",
}

type LineNotificationPayload struct {
	CorrelationID string `json:"correlation_id"`
	Content       string `json:"content"`
	Priority      string `json:"priority"`
	RetryLimit    int    `json:"retry_limit"`
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokers,
		Topic:   kafkaTopic,
		GroupID: "asynq-group",
	})

	defer r.Close()

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("❌ Error reading from Kafka: %v", err)
		}

		log.Printf("📩 Received from Kafka: %s\n", string(msg.Value))

		var jobData LineNotificationPayload
		if err := json.Unmarshal(msg.Value, &jobData); err != nil {
			log.Printf("❌ Error parsing message: %v", err)
			continue
		}

		task := asynq.NewTask("ProcessJob", msg.Value)

		_, err = client.Enqueue(task, asynq.Queue(jobData.Priority), asynq.MaxRetry(3))
		if err != nil {
			log.Fatalf("❌ Failed to enqueue job: %v", err)
		}

		log.Printf("✅ Job enqueued to Asynq (Priority: %s)\n", jobData.Priority)
	}
}
