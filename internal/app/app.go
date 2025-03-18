package app

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log/slog"
	"payments-worker/internal/config"
	"payments-worker/internal/infrastructure/kafka/dlq"
	"payments-worker/internal/infrastructure/kafka/purchase"
	chrep "payments-worker/internal/infrastructure/repository/clickhouse"
	"payments-worker/internal/metrics"
	"payments-worker/internal/service"
)

type PurchaseConsumer interface {
	Consume(ctx context.Context) error
	Close() error
}

type DLQProducer interface {
	Close() error
}

type Repository interface {
	Close() error
}

type Service interface {
	Shutdown()
}

type Worker struct {
	log         *slog.Logger
	consumer    PurchaseConsumer
	dlqProducer DLQProducer
	repository  Repository
	service     Service
}

func NewWorker(cfg config.Config, log *slog.Logger) (*Worker, error) {
	// init metrics Prometheus
	metrics.Register()

	// init conn to ClickHouse
	conn, err := initClickHouseConnection(cfg, log)
	if err != nil {
		return nil, err
	}

	// temporary migration solution (TODO: replace with full-featured migrations)
	if err := workaroundMigration(conn); err != nil {
		log.Error("failed to create table purchases", slog.Any("error", err))
		return nil, err
	}

	// init repository
	purchaseRepo := chrep.NewPurchaseRepository(conn, log)

	// init kafka dlq producer
	dlqProducer, err := dlq.NewDLQProducer(cfg.Kafka, log)
	if err != nil {
		log.Error("failed to create kafka dlq producer", slog.Any("error", err))
		return nil, err
	}

	// init service
	purchaseServ := service.NewPurchaseService(purchaseRepo, dlqProducer, cfg.ServiceConfig, log)

	// init kafka consumer
	purchaseConsumer, err := purchase.NewPurchaseConsumer(cfg.Kafka, log, purchaseServ)
	if err != nil {
		log.Error("failed to create kafka purchase consumer", slog.Any("error", err))
		return nil, err
	}

	return &Worker{
		log:         log,
		consumer:    purchaseConsumer,
		dlqProducer: dlqProducer,
		repository:  purchaseRepo,
		service:     purchaseServ,
	}, nil
}

func initClickHouseConnection(cfg config.Config, log *slog.Logger) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.ConnectionStrings.PurchaseClickHouse.Host, cfg.ConnectionStrings.PurchaseClickHouse.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.ConnectionStrings.PurchaseClickHouse.Database,
			Username: cfg.ConnectionStrings.PurchaseClickHouse.Username,
			Password: cfg.ConnectionStrings.PurchaseClickHouse.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": cfg.ConnectionStrings.PurchaseClickHouse.MaxExecutionTime,
		},
		Compression: &clickhouse.Compression{
			Method: getCompressionMethod(cfg.ConnectionStrings.PurchaseClickHouse.CompressionMethod),
		},
		DialTimeout:     cfg.ConnectionStrings.PurchaseClickHouse.DialTimeout,
		MaxOpenConns:    cfg.ConnectionStrings.PurchaseClickHouse.MaxOpenConns,
		MaxIdleConns:    cfg.ConnectionStrings.PurchaseClickHouse.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnectionStrings.PurchaseClickHouse.ConnMaxLifetime,
		BlockBufferSize: cfg.ConnectionStrings.PurchaseClickHouse.BlockBufferSize,
	})
	if err != nil {
		log.Error("failed to connect to database", slog.Any("error", err))
		return nil, err
	}

	if err = conn.Ping(context.Background()); err != nil {
		log.Error("failed to ping database", slog.Any("error", err))
		return nil, err
	}

	return conn, nil
}

func (w *Worker) Run() error {
	w.log.Info("worker started, consuming messages...")
	return w.consumer.Consume(context.Background())
}

// TODO: consider using the cancellation context
func (w *Worker) Shutdown(ctx context.Context) error {
	w.log.Info("shutting down worker...")

	if err := w.consumer.Close(); err != nil {
		w.log.Error("failed to close message consumer", slog.Any("error", err))
	}

	// Завершаем сервис перед закрытием репозитория
	w.service.Shutdown()

	if err := w.repository.Close(); err != nil {
		w.log.Error("failed to close repository", slog.Any("error", err))
	}

	if err := w.dlqProducer.Close(); err != nil {
		w.log.Error("failed to close message producer", slog.Any("error", err))
	}

	w.log.Info("worker stopped")
	return nil
}

func getCompressionMethod(method string) clickhouse.CompressionMethod {
	switch method {
	case "none":
		return clickhouse.CompressionNone
	case "zstd":
		return clickhouse.CompressionZSTD
	case "lz4":
		return clickhouse.CompressionLZ4
	case "lz4hc":
		return clickhouse.CompressionLZ4HC
	case "gzip":
		return clickhouse.CompressionGZIP
	case "deflate":
		return clickhouse.CompressionDeflate
	case "br":
		return clickhouse.CompressionBrotli
	default:
		return clickhouse.CompressionNone
	}
}

// TODO: implement migrations and remove this
func workaroundMigration(conn driver.Conn) error {
	return conn.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS purchases (
			purchase_id String,
			user_id UInt64,
			trainer_id UInt64,
			timestamp DateTime,
			training_type String,
			training_id UInt64,
			training_title String,
			training_category String,
			price_cents UInt32,
			discount_cents UInt32,
			session_date Date,
			payment_method String,
			payment_status String,
			payment_amount_cents UInt32,
			payment_currency String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (timestamp, user_id);`)
}
