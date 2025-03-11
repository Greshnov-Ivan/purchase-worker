package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"payments-worker/internal/config"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	batchSize    = 500
	concurrency  = 10
	jsonFilePath = "internal/testdata/purchases.txt"
	frequency    = 500 * time.Millisecond
)

func main() {
	cfg := config.MustLoad()

	saramaConfig := createSaramaConfig()
	producer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("failed to create Kafka producer: %v", err)
	}
	defer closeProducer(producer)

	file, err := os.Open(jsonFilePath)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer closeFile(file)

	messageCh := make(chan []byte, batchSize*concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker(producer, messageCh, &wg, cfg.Topic)
	}

	processFile(file, messageCh)

	close(messageCh)
	wg.Wait()

	log.Println("message processing completed!")
}

func createSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Messages = batchSize
	config.Producer.Flush.Frequency = frequency
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}

func closeProducer(producer sarama.AsyncProducer) {
	if err := producer.Close(); err != nil {
		log.Printf("failed to close producer: %v", err)
	}
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Printf("failed to close file: %v", err)
	}
}

func worker(producer sarama.AsyncProducer, messageCh <-chan []byte, wg *sync.WaitGroup, topic string) {
	defer wg.Done()
	for msg := range messageCh {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msg),
		}
	}
}

func processFile(file *os.File, messageCh chan<- []byte) {
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var jsonData json.RawMessage
		if err := json.Unmarshal(scanner.Bytes(), &jsonData); err != nil {
			log.Printf("error parsing JSON: %v", err)
			continue
		}
		messageCh <- append([]byte(nil), scanner.Bytes()...)
	}

	if err := scanner.Err(); err != nil {
		log.Printf("error reading file: %v", err)
	}
}
