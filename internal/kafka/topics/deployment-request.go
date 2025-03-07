package topics

import (
	"deployment-service/internal/config"
	"log/slog"
)

// NewDeploymentRequestHandler создает новый обработчик с настройками из конфигурации
func NewDeploymentRequestHandler(log *slog.Logger, topic config.TopicConfig) Handler {
	if !topic.Enabled {
		log.Warn("deployment-request handler is disabled in config")
		return nil
	}

	return &DeploymentRequestHandler{
		logger: log,
		topic:  topic.Name,
	}
}

// DeploymentRequestHandler обрабатывает сообщения о новых развертываниях
type DeploymentRequestHandler struct {
	logger *slog.Logger
	topic  string
}

// Topic возвращает название топика
func (h *DeploymentRequestHandler) Topic() string {
	return h.topic
}

// HandleMessage обрабатывает сообщение
func (h *DeploymentRequestHandler) HandleMessage(key []byte, value []byte) error {
	h.logger.Info("Handling deployment-requests with config",
		"topic", h.topic)

	return nil
}
