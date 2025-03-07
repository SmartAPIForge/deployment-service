package kafka

import (
	"deployment-service/internal/kafka/topics"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log/slog"
	"sync"
)

type Consumer struct {
	consumer *kafka.Consumer
	logger   *slog.Logger
	handlers map[string]topics.Handler
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewConsumer(log *slog.Logger, bootstrapServers string, groupID string, handlers []topics.Handler) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "smallest",
		"enable.auto.commit": false,
	})

	if err != nil {
		return nil, err
	}

	handlersMap := make(map[string]topics.Handler)
	for _, h := range handlers {
		if h != nil {
			handlersMap[h.Topic()] = h
		}
	}

	return &Consumer{
		consumer: consumer,
		logger:   log,
		handlers: handlersMap,
		stopChan: make(chan struct{}),
	}, nil
}

func (c *Consumer) Start() error {
	if len(c.handlers) == 0 {
		c.logger.Warn("No message handlers registered, consumer won't subscribe to any topics")
		return nil
	}

	topicsHandler := make([]string, 0, len(c.handlers))
	for topic := range c.handlers {
		topicsHandler = append(topicsHandler, topic)
	}

	err := c.consumer.SubscribeTopics(topicsHandler, nil)
	if err != nil {
		return err
	}

	c.logger.Info("Subscribed to topics", "topics", topicsHandler)

	c.wg.Add(1)
	go c.processMessages()

	return nil
}

func (c *Consumer) Stop() {
	close(c.stopChan)
	c.wg.Wait()
}

// handleKafkaMessage обрабатывает одно Kafka сообщение
func (c *Consumer) handleKafkaMessage(msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic

	handler, exists := c.handlers[topic]
	if !exists {
		c.logger.Warn("No handler for topic", "topic", topic)
		c.commitMessage(msg)
		return
	}

	err := handler.HandleMessage(msg.Key, msg.Value)
	if err != nil {
		c.logger.Error("Failed to process message",
			"topic", topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
			"error", err)
	} else {
		c.logger.Debug("Successfully processed message",
			"topic", topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset)
	}

	c.commitMessage(msg)
}

func (c *Consumer) commitMessage(msg *kafka.Message) {
	_, err := c.consumer.CommitMessage(msg)
	if err != nil {
		c.logger.Error("Failed to commit message",
			"topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
			"error", err)
	}
}

func (c *Consumer) processMessages() {
	defer c.wg.Done()
	defer func() {
		c.logger.Info("Closing Kafka consumer")
		if err := c.consumer.Close(); err != nil {
			c.logger.Error("Error closing Kafka consumer", "error", err)
		}
	}()

	c.logger.Info("Started processing Kafka messages")

	run := true
	for run {
		select {
		case <-c.stopChan:
			c.logger.Info("Received stop signal for Kafka consumer")
			run = false

		default:
			ev := c.consumer.Poll(100)

			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.handleKafkaMessage(e)

			case kafka.Error:
				if e.Code() != kafka.ErrPartitionEOF {
					c.logger.Error("Kafka error", "code", e.Code(), "error", e)

					if e.IsFatal() {
						c.logger.Error("Fatal Kafka error, stopping consumer")
						run = false
					}
				}

			case kafka.AssignedPartitions:
				c.logger.Info("Partitions assigned", "partitions", e.Partitions)
				err := c.consumer.Assign(e.Partitions)
				if err != nil {
					c.logger.Error("Failed to assign partitions", "error", err)
				}

			case kafka.RevokedPartitions:
				c.logger.Info("Partitions revoked", "partitions", e.Partitions)
				err := c.consumer.Unassign()
				if err != nil {
					c.logger.Error("Failed to unassign partitions", "error", err)
				}

			default:
				// Ignore other events.
			}
		}
	}

	c.logger.Info("Stopped processing Kafka messages")
}
