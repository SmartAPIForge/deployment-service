package kafka

import (
	"deployment-service/internal/config"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	producer *kafka.Producer
	logger   *slog.Logger
}

type ProjectStatus struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type DeployPayload struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
	URL   string `json:"url"`
}

func NewProducer(log *slog.Logger, bootstrapServers string) (*Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer: producer,
		logger:   log,
	}, nil
}

func (p *Producer) PublishProjectStatus(id string, status string) error {
	msg := ProjectStatus{
		ID:     id,
		Status: status,
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal project status: %w", err)
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	topic := config.ProjectStatusTopic
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

func (p *Producer) PublishDeployPayload(owner, name, url string) error {
	msg := DeployPayload{
		Owner: owner,
		Name:  name,
		URL:   url,
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal deploy payload: %w", err)
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	topic := config.DeployPayloadTopic
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("failed to produce msg: %w", err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

func (p *Producer) Close() {
	p.producer.Close()
}
