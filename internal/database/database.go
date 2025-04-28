package database

import (
	"deployment-service/internal/config"
	"deployment-service/internal/model"
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log/slog"
)

// Service holds the database connection
type Service struct {
	db     *gorm.DB
	logger *slog.Logger
}

// NewService creates a new database service
func NewService(postgresDb config.PostgresDbConfig, log *slog.Logger) (*Service, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d",
		postgresDb.Host,
		postgresDb.User,
		postgresDb.Pass,
		postgresDb.Name,
		postgresDb.Port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.AutoMigrate(&model.Server{}); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	log.Info("Database initialized", slog.String("path", dsn))
	return &Service{
		db:     db,
		logger: log,
	}, nil
}

// GetDB returns the GORM database connection
func (s *Service) GetDB() *gorm.DB {
	return s.db
}

// Close closes the database connection
func (s *Service) Close() {
	sqlDB, err := s.db.DB()
	if err != nil {
		s.logger.Error("Failed to get SQL DB", slog.String("error", err.Error()))
		return
	}

	if err := sqlDB.Close(); err != nil {
		s.logger.Error("Failed to close database", slog.String("error", err.Error()))
	}
}
