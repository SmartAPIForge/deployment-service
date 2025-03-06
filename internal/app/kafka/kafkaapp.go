package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log/slog"
)

type Consumer struct {
	consumer *kafka.Consumer
	logger   *slog.Logger
	topics   []string
	stopChan chan struct{}
}

func NewConsumer(log *slog.Logger, bootstrapServers string, groupID string, topics []string) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "smallest",
		"enable.auto.commit": false,
	})

	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: consumer,
		logger:   log,
		topics:   topics,
		stopChan: make(chan struct{}),
	}, nil
}

func (c *Consumer) Start() error {
	err := c.consumer.SubscribeTopics(c.topics, nil)
	if err != nil {
		return err
	}

	c.logger.Info("Subscribed to topics", "topics", c.topics)

	go c.processMessages()

	return nil
}

func (c *Consumer) Stop() {
	close(c.stopChan)
}

func (c *Consumer) processMessages() {
	run := true
	for run == true {
		ev := c.consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			c.logger.Info("Received message", "topic", *e.TopicPartition.Topic, "key", string(e.Key), "value", string(e.Value))
		case kafka.Error:
			c.logger.Error("Error", "error", e)
			run = false
		default:
			// Ignored event.
		}
	}

	err := c.consumer.Close()
	if err != nil {
		return
	}
}
