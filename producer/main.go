package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hibiken/asynq"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaTopic = "asynq_jobs"
)

const redisAddr = "localhost:6379"

var kafkaBrokers = []string{
	"localhost:9092",
	// "connect-kafka-dev01.devcloud.scb:19092",
	// "connect-kafka-dev02.devcloud.scb:19092",
	// "connect-kafka-dev03.devcloud.scb:19092",
}

type JobPayload struct {
	Message  string `json:"message"`
	Priority string `json:"priority"` // high, medium, low
}

func main() {

	// Step 1: Ensure topic exists before producing messages
	err := ensureTopicExists(kafkaBrokers[0], kafkaTopic)
	if err != nil {
		log.Fatalf("❌ Failed to verify/create topic: %v", err)
	}

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer client.Close()

	priorities := []string{"high", "medium", "low"}

	for i := 1; i <= 10; i++ {
		priority := priorities[rand.Intn(len(priorities))]
		message := fmt.Sprintf("Job #%d", i)

		jobData, _ := json.Marshal(JobPayload{Message: message, Priority: priority})
		task := asynq.NewTask("ProcessJob", jobData)

		//_, err := client.Enqueue(task, asynq.Queue(priority), asynq.MaxRetry(1))
		_, err := client.Enqueue(task, asynq.Queue(priority))
		if err != nil {
			log.Fatalf("❌ Failed to enqueue job: %v", err)
		}

		log.Printf("✅ Enqueued job: %s (Priority: %s)\n", message, priority)
		time.Sleep(1 * time.Second)
	}
}

// Function to check if the topic exists, and create it if it doesn't
func ensureTopicExists(kafkaBroker, topic string) error {
	conn, err := kafka.Dial("tcp", kafkaBroker)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka broker: %v", err)
	}
	defer conn.Close()

	// Check if the topic already exists
	exists, err := topicExists(conn, topic)
	if err != nil {
		return err
	}

	if exists {
		log.Println("✅ Topic already exists:", topic)
		return nil
	}

	// Create topic if it does not exist
	log.Println("🚀 Creating topic:", topic)
	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3, // Set number of partitions
		ReplicationFactor: 1, // Ensure at least 1 copy
	}

	err = conn.CreateTopics(topicConfig)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	log.Println("✅ Topic created successfully:", topic)
	return nil
}

// Function to check if a Kafka topic exists
func topicExists(conn *kafka.Conn, topicName string) (bool, error) {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return false, err
	}

	for _, p := range partitions {
		if p.Topic == topicName {
			return true, nil
		}
	}
	return false, nil
}
