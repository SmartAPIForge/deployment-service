package app

import (
	grpcapp "deployment-service/internal/app/grpc"
	kafkaapp "deployment-service/internal/app/kafka"
	"deployment-service/internal/config"
	"deployment-service/internal/database"
	deploymentserver "deployment-service/internal/grpc/deployment"
	"deployment-service/internal/kafka/topics"
	"fmt"
	"log/slog"
	"strings"
)

type App struct {
	GRPCServer    *grpcapp.GrpcApp
	KafkaConsumer *kafkaapp.Consumer
	dbService     *database.Service
	logger        *slog.Logger
}

func NewApp(
	log *slog.Logger,
	cfg *config.Config,
) (*App, error) {
	dbService, err := database.NewService(cfg.PostgresDb, log)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize deployment service
	deploymentService := deploymentserver.NewDeploymentServer(dbService.GetDB(), log)

	// Initialize Kafka consumer
	deploymentRequestHandler := topics.NewDeploymentRequestHandler(dbService.GetDB(), log, cfg.Kafka.Topics.DeploymentRequest)

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
		dbService:     dbService,
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
	a.dbService.Close()
}
