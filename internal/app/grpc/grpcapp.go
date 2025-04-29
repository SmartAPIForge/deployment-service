package grpcapp

import (
	deploymentserver "deployment-service/internal/grpc/deployment"
	interceptorlogger "deployment-service/internal/interceptors"
	"fmt"
	"log/slog"
	"net"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc"
)

type GrpcApp struct {
	log        *slog.Logger
	gRPCServer *grpc.Server
	port       int
}

func NewGrpcApp(log *slog.Logger, deploymentService *deploymentserver.Server, port int) *GrpcApp {
	loggingOpts := []logging.Option{
		logging.WithLogOnEvents(
			logging.PayloadReceived, logging.PayloadSent,
		),
	}
	recoveryOpts := []recovery.Option{
		recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			log.Error("Recovered from panic", slog.Any("panic", p))
			return status.Errorf(codes.Internal, "internal server error")
		}),
	}
	gRPCServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		recovery.UnaryServerInterceptor(recoveryOpts...),
		logging.UnaryServerInterceptor(interceptorlogger.InterceptorLogger(log), loggingOpts...),
	))

	// Register the deployment service
	deploymentserver.RegisterDeploymentServer(gRPCServer, deploymentService)

	return &GrpcApp{
		log:        log,
		gRPCServer: gRPCServer,
		port:       port,
	}
}

func (a *GrpcApp) MustRun() {
	const op = "grpcapp.MustRun"

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", a.port))
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err))
	}

	a.log.Info("gRPC server started", slog.String("addr", l.Addr().String()))

	go func() {
		if err := a.gRPCServer.Serve(l); err != nil {
			panic(fmt.Errorf("%s: %w", op, err))
		}
	}()
}

func (a *GrpcApp) Stop() {
	const op = "grpcapp.Stop"

	a.log.With(slog.String("op", op)).
		Info("stopping gRPC server", slog.Int("port", a.port))

	a.gRPCServer.GracefulStop()
}
