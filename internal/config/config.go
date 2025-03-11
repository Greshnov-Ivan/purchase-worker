package config

import (
	"github.com/ilyakaznacheev/cleanenv"
	"log"
	"os"
	"time"
)

type Config struct {
	Env               string `yaml:"env" env-required:"true"`
	ConnectionStrings `yaml:"connection_strings"`
	App               `yaml:"app"`
	Prometheus        `yaml:"prometheus"`
	Kafka             `yaml:"kafka"`
}

type ConnectionStrings struct {
	PurchaseClickHouse `yaml:"purchase_clickhouse"`
}

type PurchaseCH struct {
	Addr *string `yaml:"addr" env-required:"true"`
}

type App struct {
	GracefulShutdownTimeout time.Duration `yaml:"graceful_shutdown_timeout" env-default:"15s"`
	ServiceConfig           `yaml:"service_config"`
}

type ServiceConfig struct {
	RetrySaveBatchConfig `yaml:"retry_save_batch_config"`
	BatchSize            int           `yaml:"batch_size" env-required:"true"`
	FlushInterval        time.Duration `yaml:"flush_interval" env-required:"true"`
	WorkerCount          int           `yaml:"worker_count" env-required:"true"`
}

type RetrySaveBatchConfig struct {
	Attempts uint          `yaml:"attempts" env-default:"5"`
	Delay    time.Duration `yaml:"delay" env-default:"200ms"`
	MaxDelay time.Duration `yaml:"max_delay" env-default:"2s"`
}

type Prometheus struct {
	HOST string `yaml:"host" env-required:"true"`
	PORT uint   `yaml:"port" env-required:"true"`
}

type PurchaseClickHouse struct {
	Host              string        `yaml:"host" env-required:"true"`
	Port              int           `yaml:"port" env-required:"true"`
	Database          string        `yaml:"database" env-required:"true"`
	Username          string        `yaml:"username" env-required:"true"`
	Password          string        `yaml:"password" env-required:"true"`
	MaxExecutionTime  int           `yaml:"max_execution_time" env-required:"true"`
	CompressionMethod string        `yaml:"compression_method" env-required:"true"`
	DialTimeout       time.Duration `yaml:"dial_timeout" env-required:"true"`
	MaxOpenConns      int           `yaml:"max_open_conns" env-required:"true"`
	MaxIdleConns      int           `yaml:"max_idle_conns" env-required:"true"`
	ConnMaxLifetime   time.Duration `yaml:"conn_max_lifetime" env-required:"true"`
	BlockBufferSize   uint8         `yaml:"block_buffer_size" env-required:"true"`
	RetryConnAttempts uint          `yaml:"retry_conn_attempts" env-default:"3"`
	RetryConnDelay    time.Duration `yaml:"retry_conn_delay" env-default:"1s"`
	RetryConnMaxDelay time.Duration `yaml:"retry_conn_max_delay" env-default:"5s"`
}

type Kafka struct {
	Brokers      []string `yaml:"brokers" env-required:"true"`
	Version      string   `yaml:"version" env-required:"true"`
	Assignor     string   `yaml:"assignor" env-required:"true"`
	GroupID      string   `yaml:"group_id" env-required:"true"`
	Topic        string   `yaml:"topic" env-required:"true"`
	DLQTopic     string   `yaml:"dlq_topic" env-required:"true"`
	Oldest       bool     `yaml:"oldest" env-required:"true"`
	ReturnErrors bool     `yaml:"return_errors" env-required:"true"`
}

func MustLoad() (cfg Config) {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONFIG_PATH environment variable not set")
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatal("CONFIG_PATH does not exist")
	}

	err := cleanenv.ReadConfig(configPath, &cfg)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	return
}
