package deployment

import (
	"context"
	"deployment-service/internal/model"
	"log/slog"
	"time"

	pb "github.com/SmartAPIForge/protos/gen/go/deployment"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

type Server struct {
	pb.UnimplementedDeploymentServiceServer
	db     *gorm.DB
	logger *slog.Logger
}

func RegisterDeploymentServer(gRPCServer *grpc.Server, server *Server) {
	pb.RegisterDeploymentServiceServer(gRPCServer, server)
}

func NewDeploymentServer(db *gorm.DB, logger *slog.Logger) *Server {
	return &Server{
		db:     db,
		logger: logger,
	}
}

func (s *Server) ListServers(ctx context.Context, req *pb.ListServersRequest) (*pb.ListServersResponse, error) {
	var servers []model.Server

	result := s.db.Find(&servers)
	if result.Error != nil {
		s.logger.Error("Failed to list servers", slog.String("error", result.Error.Error()))
		return &pb.ListServersResponse{}, result.Error
	}

	previewServers := make([]*pb.ServerPreview, len(servers))
	for i, server := range servers {
		previewServers[i] = server.ToPreviewProto()
	}

	return &pb.ListServersResponse{
		Servers: previewServers,
	}, nil
}

func (s *Server) AddServer(ctx context.Context, req *pb.AddServerRequest) (*pb.AddServerResponse, error) {
	newServer := model.FromAddRequest(req)

	result := s.db.Create(newServer)
	if result.Error != nil {
		s.logger.Error("Failed to add server", slog.String("error", result.Error.Error()))
		return &pb.AddServerResponse{}, result.Error
	}

	return &pb.AddServerResponse{Server: newServer.ToPreviewProto()}, nil
}

func (s *Server) RemoveServer(ctx context.Context, req *pb.RemoveServerRequest) (*pb.RemoveServerResponse, error) {
	result := s.db.Delete(&model.Server{}, req.Id)

	if result.Error != nil {
		s.logger.Error("Failed to remove server", slog.String("error", result.Error.Error()),
			slog.Uint64("server_id", uint64(req.Id)))
		return &pb.RemoveServerResponse{Status: false}, result.Error
	}

	// Check if any row was affected
	return &pb.RemoveServerResponse{
		Status: result.RowsAffected > 0,
	}, nil
}

func (s *Server) GetDeployment(ctx context.Context, req *pb.GetDeploymentRequest) (*pb.GetDeploymentResponse, error) {
	var deployment model.Deployment
	result := s.db.First(&deployment, "id = ?", req.Id)
	if result.Error != nil {
		s.logger.Error("Failed to get deployment", slog.String("error", result.Error.Error()),
			slog.String("deployment_id", req.Id))
		return &pb.GetDeploymentResponse{}, result.Error
	}

	return &pb.GetDeploymentResponse{
		Deployment: deployment.ToProto(),
	}, nil
}

func (s *Server) ListDeployments(ctx context.Context, req *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	var deployments []model.Deployment
	query := s.db

	if req.Owner != "" {
		query = query.Where("owner = ?", req.Owner)
	}

	result := query.Find(&deployments)
	if result.Error != nil {
		s.logger.Error("Failed to list deployments", slog.String("error", result.Error.Error()))
		return &pb.ListDeploymentsResponse{}, result.Error
	}

	protoDeployments := make([]*pb.Deployment, len(deployments))
	for i, deployment := range deployments {
		protoDeployments[i] = deployment.ToProto()
	}

	return &pb.ListDeploymentsResponse{
		Deployments: protoDeployments,
	}, nil
}

func (s *Server) DeleteDeployment(ctx context.Context, req *pb.DeleteDeploymentRequest) (*pb.DeleteDeploymentResponse, error) {
	result := s.db.Delete(&model.Deployment{}, "id = ?", req.Id)
	if result.Error != nil {
		s.logger.Error("Failed to delete deployment", slog.String("error", result.Error.Error()),
			slog.String("deployment_id", req.Id))
		return &pb.DeleteDeploymentResponse{Success: false}, result.Error
	}

	return &pb.DeleteDeploymentResponse{
		Success: result.RowsAffected > 0,
	}, nil
}

func (s *Server) StartDeployment(ctx context.Context, req *pb.StartDeploymentRequest) (*pb.StartDeploymentResponse, error) {
	var deployment model.Deployment
	result := s.db.First(&deployment, "id = ?", req.Id)
	if result.Error != nil {
		s.logger.Error("Failed to find deployment", slog.String("error", result.Error.Error()),
			slog.String("deployment_id", req.Id))
		return &pb.StartDeploymentResponse{}, result.Error
	}

	// Update deployment status and start time
	deployment.Status = "running"
	deployment.StartTime = time.Now()

	result = s.db.Save(&deployment)
	if result.Error != nil {
		s.logger.Error("Failed to start deployment", slog.String("error", result.Error.Error()),
			slog.String("deployment_id", req.Id))
		return &pb.StartDeploymentResponse{}, result.Error
	}

	return &pb.StartDeploymentResponse{
		Deployment: deployment.ToProto(),
	}, nil
}

func (s *Server) StopDeployment(ctx context.Context, req *pb.StopDeploymentRequest) (*pb.StopDeploymentResponse, error) {
	var deployment model.Deployment
	result := s.db.First(&deployment, "id = ?", req.Id)
	if result.Error != nil {
		s.logger.Error("Failed to find deployment", slog.String("error", result.Error.Error()),
			slog.String("deployment_id", req.Id))
		return &pb.StopDeploymentResponse{}, result.Error
	}

	// Update deployment status and end time
	deployment.Status = "stopped"
	deployment.EndTime = time.Now()
	deployment.Duration = deployment.EndTime.Sub(deployment.StartTime)

	result = s.db.Save(&deployment)
	if result.Error != nil {
		s.logger.Error("Failed to stop deployment", slog.String("error", result.Error.Error()),
			slog.String("deployment_id", req.Id))
		return &pb.StopDeploymentResponse{}, result.Error
	}

	return &pb.StopDeploymentResponse{
		Deployment: deployment.ToProto(),
	}, nil
}
