package filetransferserver

import (
	"context"

	pb "github.com/SmartAPIForge/protos/gen/go/deployment"
	"google.golang.org/grpc"
)

type DeploymentServer struct {
	pb.UnimplementedDeploymentServiceServer
	servers []*pb.Server
}

func RegisterDeploymentServer(gRPCServer *grpc.Server) {
	deploymentServer := NewDeploymentServer()
	pb.RegisterDeploymentServiceServer(gRPCServer, deploymentServer)
}

func NewDeploymentServer() *DeploymentServer {
	return &DeploymentServer{}
}

func (s *DeploymentServer) ListServers(ctx context.Context, req *pb.ListServersRequest) (*pb.ListServersResponse, error) {
	previewServers := make([]*pb.ServerPreview, len(s.servers))

	for i, server := range s.servers {
		previewServers[i] = &pb.ServerPreview{
			Id:   server.Id,
			Ip:   server.Ip,
			Port: server.Port,
			User: server.User,
		}
	}

	return &pb.ListServersResponse{
		Servers: previewServers,
	}, nil
}

func (s *DeploymentServer) AddServer(ctx context.Context, req *pb.AddServerRequest) (*pb.AddServerResponse, error) {
	var id uint32 = 1
	if len(s.servers) > 0 {
		id = s.servers[len(s.servers)-1].Id + 1
	}

	newServer := &pb.Server{
		Id:       id,
		Ip:       req.Ip,
		Port:     req.Port,
		User:     req.User,
		Password: req.Password,
	}
	s.servers = append(s.servers, newServer)

	previewServer := &pb.ServerPreview{
		Id:   id,
		Ip:   req.Ip,
		Port: req.Port,
		User: req.User,
	}
	return &pb.AddServerResponse{Server: previewServer}, nil
}

func (s *DeploymentServer) RemoveServer(ctx context.Context, req *pb.RemoveServerRequest) (*pb.RemoveServerResponse, error) {
	response := &pb.RemoveServerResponse{
		Status: false,
	}

	for i, server := range s.servers {
		if server.Id == req.Id {
			s.servers = append(s.servers[:i], s.servers[i+1:]...)
			response.Status = true
			return response, nil
		}
	}

	return response, nil
}
