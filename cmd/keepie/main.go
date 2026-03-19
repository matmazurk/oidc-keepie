package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/matmazurk/oidc-keepie/api"
	"github.com/matmazurk/oidc-keepie/handler"
	"github.com/matmazurk/oidc-keepie/kafka"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := loadConfig()

	tp, err := keepieotel.SetupTracing(ctx)
	if err != nil {
		slog.Error("setting up tracing", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer tp.Shutdown(context.Background())

	mp, err := keepieotel.SetupMetrics(ctx)
	if err != nil {
		slog.Error("setting up metrics", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer mp.Shutdown(context.Background())

	producer, err := kafka.NewProducer(cfg.brokers, cfg.topic)
	if err != nil {
		slog.Error("creating producer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer producer.Close()

	handle := handler.New(&stubIssuer{}, &http.Client{})

	consumer, err := kafka.NewConsumer(cfg.brokers, cfg.groupID, cfg.topic, producer, handle)
	if err != nil {
		slog.Error("creating consumer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer consumer.Close()

	go func() {
		slog.Info("starting consumer")
		if err := consumer.Start(ctx); err != nil {
			slog.Error("consumer stopped", slog.String("error", err.Error()))
		}
	}()

	apiHandler := api.NewHandler(producer)
	mux := http.NewServeMux()
	apiHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    ":" + cfg.httpPort,
		Handler: mux,
	}

	go func() {
		slog.Info("starting HTTP server", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", slog.String("error", err.Error()))
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	if err := server.Shutdown(context.Background()); err != nil {
		slog.Error("HTTP server shutdown", slog.String("error", err.Error()))
	}
}

type stubIssuer struct{}

func (s *stubIssuer) Issue(_ context.Context) ([]byte, error) {
	return []byte("placeholder-token"), nil
}
