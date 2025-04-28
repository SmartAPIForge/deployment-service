package deployment

import (
	"context"
	"deployment-service/internal/model"
	"log/slog"

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
