package app

import (
	grpcapp "deployment-service/internal/app/grpc"
	kafkaapp "deployment-service/internal/app/kafka"
	"deployment-service/internal/config"
	deploymentserver "deployment-service/internal/grpc/deployment"
	"deployment-service/internal/kafka/topics"
	"log/slog"
	"strings"
)

type App struct {
	GRPCServer    *grpcapp.GrpcApp
	KafkaConsumer *kafkaapp.Consumer
	logger        *slog.Logger
}

func NewApp(
	log *slog.Logger,
	cfg *config.Config,
) (*App, error) {
	deploymentService := deploymentserver.DeploymentServer{}

	deploymentRequestHandler := topics.NewDeploymentRequestHandler(log, cfg.Kafka.Topics.DeploymentRequest)

	consumer, err := kafkaapp.NewConsumer(
		log,
		strings.Join(cfg.Kafka.BootstrapServers, ","),
		"deployment-consumer",
		[]topics.Handler{deploymentRequestHandler},
	)
	if err != nil {
		return nil, err
	}

	return &App{
		GRPCServer:    grpcapp.NewGrpcApp(log, deploymentService, cfg.GRPC.Port),
		KafkaConsumer: consumer,
		logger:        log,
	}, nil
}

func (a *App) Start() error {
	a.logger.Info("Starting application components")

	a.GRPCServer.MustRun()

	err := a.KafkaConsumer.Start()
	if err != nil {
		a.logger.Error("Error starting Kafka consumer", "error", err)
		a.GRPCServer.Stop()
		return err
	}

	return nil
}

func (a *App) Stop() {
	a.logger.Info("Stopping application components")
	a.KafkaConsumer.Stop()
	a.GRPCServer.Stop()
}
