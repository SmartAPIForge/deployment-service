package topics

import "deployment-service/internal/config"

type Handler interface {
	Topic() string
	HandleMessage(key []byte, value []byte) error
}

type HandleFactory interface {
	Create(cfg config.TopicConfig) (Handler, error)
}
