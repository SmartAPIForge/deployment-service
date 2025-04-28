package config

import (
	"flag"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	EnvLocal = "local"
	EnvDev   = "dev"
	EnvProd  = "prod"
)

const (
	ProjectStatusTopic = "ProjectStatus"
	DeployPayloadTopic = "DeployPayload"
)

type Config struct {
	Env        string           `yaml:"env" env-default:"local"`
	GRPC       GRPCConfig       `yaml:"grpc"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	PostgresDb PostgresDbConfig `yaml:"postgres_db"`
}

type PostgresDbConfig struct {
	Host string `yaml:"host" env-required:"true"`
	Port uint   `yaml:"port" env-required:"true"`
	User string `yaml:"user" env-required:"true"`
	Pass string `yaml:"password" env-required:"true"`
	Name string `yaml:"db_name" env-required:"true"`
}

type TestServerConfig struct {
	Host       string `yaml:"host"`
	User       string `yaml:"user"`
	PrivateKey string `yaml:"private_key"`
}

type TopicConfig struct {
	Name    string `yaml:"name"`
	Enabled bool   `yaml:"enabled"`
}

type TopicsConfig struct {
	DeploymentRequest TopicConfig `yaml:"deployment_request"`
}

type KafkaConfig struct {
	BootstrapServers []string     `yaml:"servers" env-required:"true"`
	Topics           TopicsConfig `yaml:"topics"`
}

type GRPCConfig struct {
	Port    int           `yaml:"port"`
	Timeout time.Duration `yaml:"timeout"`
}

func MustLoad() *Config {
	configPath := fetchConfigPath()
	if configPath == "" {
		panic("config path is empty")
	}

	return MustLoadPath(configPath)
}

func MustLoadPath(configPath string) *Config {
	// check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic("config file does not exist: " + configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("cannot read config: " + err.Error())
	}

	return &cfg
}

// fetchConfigPath fetches config path from command line flag or environment variable.
// Priority: flag > env > default.
// Default value is empty string.
func fetchConfigPath() string {
	var res string

	flag.StringVar(&res, "config", "", "path to config file")
	flag.Parse()

	if res == "" {
		res = os.Getenv("CONFIG_PATH")
	}

	return res
}
