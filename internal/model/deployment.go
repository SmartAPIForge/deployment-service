package model

import (
	"time"

	pb "github.com/SmartAPIForge/protos/gen/go/deployment"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Deployment struct {
	ID          string    `gorm:"primaryKey"`
	Owner       string    `gorm:"not null"`
	Name        string    `gorm:"not null"`
	URL         string    `gorm:"not null"`
	Status      string    `gorm:"not null"`
	StartTime   time.Time `gorm:"not null"`
	EndTime     time.Time
	Duration    time.Duration
	ServerID    uint32 `gorm:"not null"`
	Server      Server `gorm:"foreignKey:ServerID"`
	ContainerID string `gorm:"not null"`
}

func (*Deployment) TableName() string {
	return "deployments"
}

func (d *Deployment) ToProto() *pb.Deployment {
	return &pb.Deployment{
		Id:              d.ID,
		Owner:           d.Owner,
		Name:            d.Name,
		Url:             d.URL,
		Status:          d.Status,
		StartTime:       timestamppb.New(d.StartTime),
		EndTime:         timestamppb.New(d.EndTime),
		DurationSeconds: int64(d.Duration.Seconds()),
		ServerId:        d.ServerID,
		ContainerId:     d.ContainerID,
	}
}
