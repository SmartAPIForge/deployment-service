package app

import (
	grpcapp "deployment-service/internal/app/grpc"
	kafkaapp "deployment-service/internal/app/kafka"
	deploymentserver "deployment-service/internal/grpc/deployment"
	"log/slog"
	"time"
)

type App struct {
	GRPCServer    *grpcapp.GrpcApp
	KafkaConsumer *kafkaapp.Consumer
	logger        *slog.Logger
}

func NewApp(
	log *slog.Logger,
	grpcPort int,
	tokenTTL time.Duration,
) (*App, error) {
	deploymentService := deploymentserver.DeploymentServer{}

	consumer, err := kafkaapp.NewConsumer(
		log,
		"localhost:29092",
		"deployment-consumer",
		[]string{"NewZip"},
	)
	if err != nil {
		return nil, err
	}

	return &App{
		GRPCServer:    grpcapp.NewGrpcApp(log, deploymentService, grpcPort),
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
