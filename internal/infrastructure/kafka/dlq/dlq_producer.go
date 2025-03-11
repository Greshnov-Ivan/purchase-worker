package dlq

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"log/slog"
	"payments-worker/internal/config"
	"payments-worker/internal/metrics"
)

type DLQProducer struct {
	producer      sarama.AsyncProducer
	log           *slog.Logger
	topic         string
	originalTopic string
}

func NewDLQProducer(cfg config.Kafka, log *slog.Logger) (*DLQProducer, error) {
	if len(cfg.Brokers) == 0 {
		err := errors.New("kafka brokers list is empty")
		log.Error("invalid configuration", slog.String("error", err.Error()))
		return nil, err
	}
	if cfg.DLQTopic == "" {
		err := errors.New("kafka topic is empty")
		log.Error("invalid configuration", slog.String("error", err.Error()))
		return nil, err
	}

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		log.Error("error parsing kafka version", slog.Any("error", err))
		return nil, err
	}

	kafkaConfig := createSaramaConfig(version)
	producer, err := sarama.NewAsyncProducer(cfg.Brokers, kafkaConfig)
	if err != nil {
		log.Error("failed to create kafka producer", slog.Any("error", err))
		return nil, err
	}

	return &DLQProducer{
		producer:      producer,
		log:           log,
		topic:         cfg.DLQTopic,
		originalTopic: cfg.Topic,
	}, nil
}

func (p *DLQProducer) Send(ctx context.Context, message []byte, err error) error {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(message),
		Headers: []sarama.RecordHeader{
			{Key: []byte("Original-Topic"), Value: []byte(p.originalTopic)},
			{Key: []byte("Error"), Value: []byte(err.Error())},
		},
	}

	select {
	case p.producer.Input() <- msg:
		metrics.DLQMessages.Inc()
		return nil
	case <-ctx.Done():
		p.log.Warn("context cancelled before sending message to DLQ",
			slog.Any("error", ctx.Err()),
			slog.String("original_topic", p.originalTopic),
		)
		return ctx.Err()
	}
}

func (p *DLQProducer) Close() error {
	p.log.Info("closing Kafka producer")
	err := p.producer.Close()
	if err != nil {
		p.log.Error("failed to close Kafka producer", slog.Any("error", err))
	}
	return err
}

func createSaramaConfig(ver sarama.KafkaVersion) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = ver
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionSnappy
	return config
}
