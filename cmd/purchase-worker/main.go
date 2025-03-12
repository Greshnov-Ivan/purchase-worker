package main

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"payments-worker/internal/app"
	"payments-worker/internal/config"
	"payments-worker/internal/lib/logger/handlers/slogpretty"
	"syscall"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	// init config: cleanenv
	cfg := config.MustLoad()

	// init logger: log/slog
	log := setupLogger(cfg.Env)

	log.Info("starting worker", slog.String("env", cfg.Env))
	log.Debug("debug messages are enabled")

	// run Prometheus HTTP-server
	promAddr := fmt.Sprintf(
		"%s:%d",
		cfg.Prometheus.HOST,
		cfg.Prometheus.PORT,
	)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Info("starting Prometheus metrics server", slog.String("address", promAddr))
		if err := http.ListenAndServe(promAddr, nil); err != nil {
			log.Error("failed to start Prometheus metrics server", slog.Any("error", err))
		}
	}()

	// init worker
	worker, err := app.NewWorker(cfg, log)
	if err != nil {
		log.Error("failed to initialize worker", slog.Any("error", err))
		os.Exit(1)
	}

	// run worker
	errChan := make(chan error, 1)
	go func() {
		errChan <- worker.Run()
	}()

	log.Info("worker started")

	// processing completion signals
	done := make(chan os.Signal, 1)
	// only Linux/macOS
	//signal.Notify(done, os.Interrupt, unix.SIGINT, unix.SIGTERM)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
		log.Info("stopping worker...")
	case err := <-errChan:
		log.Error("worker crashed", slog.Any("error", err))
		os.Exit(1)
	}

	log.Info("shutting down worker...")

	// context for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), cfg.App.GracefulShutdownTimeout)
	defer cancel()

	if err := worker.Shutdown(ctx); err != nil {
		log.Error("failed to stop worker", slog.Any("error", err))
	}

	log.Info("worker stopped")
}

// configuring the logger
func setupLogger(env string) *slog.Logger {
	switch env {
	case envLocal:
		return setupPrettySlog()
	case envDev:
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	case envProd:
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// pretty logger for the local environment
func setupPrettySlog() *slog.Logger {
	opts := slogpretty.PrettyHandlerOptions{
		SlogOpts: &slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	}
	handler := opts.NewPrettyHandler(os.Stdout)
	return slog.New(handler)
}
