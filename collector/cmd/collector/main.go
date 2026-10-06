// Command collector — Go-сервис сбора рыночных данных T-Invest API (2).
// Подписывается на свечи через стрим T-Invest API, публикует их в Kafka и
// предоставляет REST API для управления списком отслеживаемых инструментов
// без перезапуска сервиса (2.7).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"charthawk/collector/internal/api"
	"charthawk/collector/internal/config"
	"charthawk/collector/internal/kafka"
	"charthawk/collector/internal/subscription"
	"charthawk/collector/internal/tinkoff"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(*configPath, logger); err != nil {
		logger.Error("collector exited with error", "error", err)
		os.Exit(1)
	}
}

func run(configPath string, logger *slog.Logger) error {
	// .env — удобство для локальной разработки: подхватываем TINKOFF_TOKEN,
	// ADMIN_API_KEY и т.п. из файла, если он есть. Переменные, уже заданные
	// в окружении процесса (Docker, Kubernetes, CI), .env не перезаписывает.
	// Отсутствие файла — не ошибка: в проде секреты обычно приходят напрямую
	// через окружение, без .env.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		logger.Warn("failed to load .env file", "error", err)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.CandlesTopic, cfg.Kafka.BatchSize, cfg.Kafka.BatchTimeout)
	defer producer.Close()

	logger.Info("connecting to T-Invest API", "endpoint", cfg.Tinkoff.EndPoint)
	tClient, err := tinkoff.Connect(ctx, cfg.Tinkoff, producer, cfg.Kafka.CandlesTopic, logger)
	if err != nil {
		return err
	}
	defer tClient.Close()

	state := subscription.NewState()
	manager := subscription.NewManager(
		state,
		tClient.Resolver(),
		tClient,
		cfg.Subscriptions.MaxPerStream,
		cfg.Subscriptions.MaxStreams,
		tinkoff.AllowedTimeframes,
	)

	if err := applyInitialSubscriptions(ctx, manager, cfg.Subscriptions.Initial); err != nil {
		return err
	}

	mux := http.NewServeMux()
	handler := api.NewHandler(manager, logger)
	handler.Routes(mux)
	mux.Handle("/metrics", promhttp.Handler())

	var chain http.Handler = mux
	chain = api.RateLimit(api.NewPutRateLimiter())(chain)
	chain = api.Auth(cfg.HTTP.AdminAPIKey)(chain)
	chain = api.Metrics(chain)

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           chain,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serveErr:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "error", err)
	}

	return nil
}

// applyInitialSubscriptions поднимает начальный список подписок из
// config.yaml (subscriptions.initial) при старте сервиса.
func applyInitialSubscriptions(ctx context.Context, manager *subscription.Manager, initial []config.SubscriptionGroup) error {
	if len(initial) == 0 {
		return nil
	}

	groups := make([]subscription.RequestGroup, len(initial))
	for i, g := range initial {
		groups[i] = subscription.RequestGroup{Timeframe: g.Timeframe, Instruments: g.Instruments}
	}

	if _, err := manager.Apply(ctx, groups); err != nil {
		return err
	}
	return nil
}
