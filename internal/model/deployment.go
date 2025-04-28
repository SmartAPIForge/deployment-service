package model

import (
	"time"
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
