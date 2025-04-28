package topics

import (
	"deployment-service/internal/config"
	"deployment-service/internal/kafka"
	"deployment-service/internal/model"
	"deployment-service/internal/ssh"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type DeploymentMessage struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
	URL   string `json:"url"`
}

// NewDeploymentRequestHandler создает новый обработчик с настройками из конфигурации
func NewDeploymentRequestHandler(db *gorm.DB, log *slog.Logger, topic config.TopicConfig, kafkaProducer *kafka.Producer) Handler {
	if !topic.Enabled {
		log.Warn("deployment-request handler is disabled in config")
		return nil
	}

	return &DeploymentRequestHandler{
		db:            db,
		logger:        log,
		topic:         topic.Name,
		kafkaProducer: kafkaProducer,
	}
}

// DeploymentRequestHandler обрабатывает сообщения о новых развертываниях
type DeploymentRequestHandler struct {
	db            *gorm.DB
	logger        *slog.Logger
	topic         string
	kafkaProducer *kafka.Producer
}

// Topic возвращает название топика
func (h *DeploymentRequestHandler) Topic() string {
	return h.topic
}

// HandleMessage обрабатывает сообщение
func (h *DeploymentRequestHandler) HandleMessage(key []byte, value []byte) error {
	h.logger.Info("Handling deployment-requests with config",
		"topic", h.topic)

	var deploymentMessage DeploymentMessage
	err := json.Unmarshal(value, &deploymentMessage)
	if err != nil {
		return err
	}

	var servers []model.Server

	result := h.db.Find(&servers)
	if result.Error != nil {
		h.logger.Error("Failed to get servers", slog.String("error", result.Error.Error()))
		return result.Error
	}

	server, err := ssh.GetOptimizedServer(servers)
	if err != nil {
		h.logger.Error("Failed to get optimized server", slog.String("error", err.Error()))
		return err
	}

	h.logger.Info("Optimized server selected", slog.String("server", server.IP))

	deploymentID := uuid.NewString()
	port := generateRandomPort()
	deploymentURL := fmt.Sprintf("http://%s:%d", server.IP, port)

	deployment := &model.Deployment{
		ID:        deploymentID,
		Owner:     deploymentMessage.Owner,
		Name:      deploymentMessage.Name,
		URL:       deploymentURL,
		Status:    "DEPLOY_PENDING",
		StartTime: time.Now(),
		ServerID:  server.ID,
	}

	if err := h.db.Create(deployment).Error; err != nil {
		h.logger.Error("Failed to create deployment record", slog.String("error", err.Error()))
		return err
	}

	if err := h.kafkaProducer.PublishProjectStatus(deploymentID, "DEPLOY_PENDING"); err != nil {
		h.logger.Error("Failed to publish deployment status", slog.String("error", err.Error()))
		return err
	}

	go func() {
		startTime := time.Now()
		err := Deploy(server, deploymentMessage, port)

		deployment.EndTime = time.Now()
		deployment.Duration = deployment.EndTime.Sub(startTime)

		if err != nil {
			h.logger.Error("Failed to deploy", slog.String("error", err.Error()))
			deployment.Status = "DEPLOY_FAIL"
			if err := h.db.Save(deployment).Error; err != nil {
				h.logger.Error("Failed to update deployment status", slog.String("error", err.Error()))
			}
			if err := h.kafkaProducer.PublishProjectStatus(deploymentID, "DEPLOY_FAIL"); err != nil {
				h.logger.Error("Failed to publish deployment status", slog.String("error", err.Error()))
			}
			return
		}

		containerID, err := ssh.GetContainerIDByPort(server, port)
		if err != nil {
			h.logger.Error("Failed to get container ID", slog.String("error", err.Error()))
			deployment.Status = "DEPLOY_FAIL"
			if err := h.db.Save(deployment).Error; err != nil {
				h.logger.Error("Failed to update deployment status", slog.String("error", err.Error()))
			}
			if err := h.kafkaProducer.PublishProjectStatus(deploymentID, "DEPLOY_FAIL"); err != nil {
				h.logger.Error("Failed to publish deployment status", slog.String("error", err.Error()))
			}
			return
		}

		deployment.Status = "DEPLOY_SUCCESS"
		deployment.ContainerID = containerID
		if err := h.db.Save(deployment).Error; err != nil {
			h.logger.Error("Failed to update deployment status", slog.String("error", err.Error()))
		}
		if err := h.kafkaProducer.PublishProjectStatus(deploymentID, "DEPLOY_SUCCESS"); err != nil {
			h.logger.Error("Failed to publish deployment status", slog.String("error", err.Error()))
		}
		if err := h.kafkaProducer.PublishDeployPayload(deploymentMessage.Owner, deploymentMessage.Name, deploymentURL); err != nil {
			h.logger.Error("Failed to publish deploy payload", slog.String("error", err.Error()))
		}
	}()

	return nil
}

func Deploy(server model.Server, message DeploymentMessage, port int) error {
	currentPath, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current path:", err)
		return err
	}

	ansibleDir := filepath.Join(currentPath, "ansible")

	inventoryContent := fmt.Sprintf(
		"[servers]\n%s ansible_user=%s ansible_ssh_pass=%s ansible_connection=ssh\n",
		server.IP, server.User, server.Password,
	)

	id := uuid.NewString()

	inventoryFile := filepath.Join(ansibleDir, id)

	file, err := os.Create(inventoryFile)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file:", err)
		}
	}(file)

	_, err = file.WriteString(inventoryContent)
	if err != nil {
		fmt.Println("Error writing inventory content:", err)
		return err
	}

	playbookFile := filepath.Join(ansibleDir, "template/playbooks/main.yml")

	s3Dest := fmt.Sprintf("%s.zip", id)
	unzipDir := id

	cmd := exec.Command(
		"ansible-playbook",
		"-i", inventoryFile,
		playbookFile,
		"-vvv",
		"--extra-vars", fmt.Sprintf("S3_URL=%s S3_DEST=%s UNZIP_DIR=%s HOST_PORT=%d",
			message.URL, s3Dest, unzipDir, port),
	)
	cmd.Dir = ansibleDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "ANSIBLE_HOST_KEY_CHECKING=False")

	return cmd.Run()
}

func generateRandomPort() int {
	return 1024 + rand.Intn(65535-1024)
}
