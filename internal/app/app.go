package app

import (
	grpcapp "deployment-service/internal/app/grpc"
	deploymentserver "deployment-service/internal/grpc/deployment"
	"log/slog"
	"time"
)

type App struct {
	GRPCServer *grpcapp.GrpcApp
}

func NewApp(
	log *slog.Logger,
	grpcPort int,
	tokenTTL time.Duration,
) *App {
	deploymentService := deploymentserver.DeploymentServer{}

	return &App{
		GRPCServer: grpcapp.NewGrpcApp(log, deploymentService, grpcPort),
	}
}
