package model

import (
	pb "github.com/SmartAPIForge/protos/gen/go/deployment"
)

// Server is the GORM model for server data
type Server struct {
	ID       uint32 `gorm:"primaryKey"`
	IP       string `gorm:"column:ip;not null"`
	Port     uint32 `gorm:"column:port;not null"`
	User     string `gorm:"column:user;not null"`
	Password string `gorm:"column:password;not null"`
}

// TableName overrides the table name
func (*Server) TableName() string {
	return "servers"
}

// ToProto converts the model to protobuf message
func (s *Server) ToProto() *pb.Server {
	return &pb.Server{
		Id:       s.ID,
		Ip:       s.IP,
		Port:     s.Port,
		User:     s.User,
		Password: s.Password,
	}
}

// ToPreviewProto converts the model to protobuf preview message
func (s *Server) ToPreviewProto() *pb.ServerPreview {
	return &pb.ServerPreview{
		Id:   s.ID,
		Ip:   s.IP,
		Port: s.Port,
		User: s.User,
	}
}

// FromProto creates a model from a protobuf message
func FromProto(server *pb.Server) *Server {
	return &Server{
		ID:       server.Id,
		IP:       server.Ip,
		Port:     server.Port,
		User:     server.User,
		Password: server.Password,
	}
}

// FromAddRequest creates a model from an add request
func FromAddRequest(req *pb.AddServerRequest) *Server {
	return &Server{
		IP:       req.Ip,
		Port:     req.Port,
		User:     req.User,
		Password: req.Password,
	}
}
