package purchase

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"log/slog"
	"payments-worker/internal/config"
)

type PurchaseService interface {
	ProcessMessage(ctx context.Context, message []byte)
}

type PurchaseConsumer struct {
	client  sarama.ConsumerGroup
	log     *slog.Logger
	service PurchaseService
	topic   string
	groupID string
}

type consumerGroupHandler struct {
	log     *slog.Logger
	service PurchaseService
}

func NewPurchaseConsumer(cfg config.Kafka, log *slog.Logger, service PurchaseService) (*PurchaseConsumer, error) {
	if len(cfg.Brokers) == 0 {
		err := errors.New("kafka brokers list is empty")
		log.Error("invalid kafka configuration", slog.String("error", err.Error()))
		return nil, err
	}

	if cfg.Topic == "" {
		err := errors.New("kafka topic is empty")
		log.Error("invalid kafka configuration", slog.String("error", err.Error()))
		return nil, err
	}

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		log.Error("error parsing kafka version",
			slog.String("version", cfg.Version),
			slog.Any("error", err),
		)
		return nil, err
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = version
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Return.Errors = cfg.ReturnErrors

	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, kafkaConfig)
	if err != nil {
		log.Error("failed to create kafka consumer group",
			slog.String("group_id", cfg.GroupID),
			slog.String("topic", cfg.Topic),
			slog.Any("error", err),
		)
		return nil, err
	}

	log.Info("kafka consumer created successfully",
		slog.String("group_id", cfg.GroupID),
		slog.String("topic", cfg.Topic),
		slog.Any("brokers", cfg.Brokers),
	)

	return &PurchaseConsumer{
		client:  client,
		log:     log,
		service: service,
		topic:   cfg.Topic,
		groupID: cfg.GroupID,
	}, nil
}

func (c *PurchaseConsumer) Consume(ctx context.Context) error {
	handler := consumerGroupHandler{
		log:     c.log.With(slog.String("component", "consumer_handler")),
		service: c.service,
	}

	c.log.Info("starting kafka consumer",
		slog.String("group_id", c.groupID),
		slog.String("topic", c.topic),
	)

	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopping consumer due to context cancellation")
			return nil
		default:
			if err := c.client.Consume(ctx, []string{c.topic}, &handler); err != nil {
				c.log.Error("kafka consumer error",
					slog.String("group_id", c.groupID),
					slog.String("topic", c.topic),
					slog.Any("error", err),
				)
				return err
			}
		}
	}
}

func (c *PurchaseConsumer) Close() error {
	c.log.Info("closing kafka consumer",
		slog.String("group_id", c.groupID),
		slog.String("topic", c.topic),
	)

	if err := c.client.Close(); err != nil {
		c.log.Error("failed to close kafka consumer",
			slog.String("group_id", c.groupID),
			slog.String("topic", c.topic),
			slog.Any("error", err),
		)
		return err
	}
	return nil
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.log.Info("consumer group setup complete",
		slog.String("member_id", session.MemberID()),
		slog.Int("generation_id", int(session.GenerationID())),
	)
	return nil
}

func (h *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.log.Info("consumer group cleanup complete",
		slog.String("member_id", session.MemberID()),
		slog.Int("generation_id", int(session.GenerationID())),
	)
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for msg := range claim.Messages() {
		h.service.ProcessMessage(ctx, msg.Value)
		// error handling and message movement in DLQ is the responsibility of ProcessMessage
		session.MarkMessage(msg, "")
	}
	return nil
}
