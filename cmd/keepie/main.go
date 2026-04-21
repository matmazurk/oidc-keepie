package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/matmazurk/oidc-keepie/api"
	"github.com/matmazurk/oidc-keepie/handler"
	"github.com/matmazurk/oidc-keepie/job"
	"github.com/matmazurk/oidc-keepie/kafka"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
	"github.com/matmazurk/oidc-keepie/pool"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

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

	tlsCfg, err := kafka.LoadTLSConfig(cfg.tlsCAFile, cfg.tlsCertFile, cfg.tlsKeyFile)
	if err != nil {
		slog.Error("loading kafka tls config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	producer, err := kafka.NewProducer(cfg.brokers, cfg.topic, tlsCfg)
	if err != nil {
		slog.Error("creating producer", slog.String("error", err.Error()))
		os.Exit(1)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	rawHandler := handler.New(&handler.StubSigner{}, handler.Config{
		Issuer:   "https://keepie.example.com",
		Audience: "https://api.example.com",
		TTL:      5 * time.Minute,
	}, httpClient)

	wrappedHandler := func(ctx context.Context, j job.Job) {
		start := time.Now()
		if err := rawHandler(ctx, j); err != nil {
			keepieotel.RecordProcessingDuration(ctx, time.Since(start))

			if job.IsRetryable(err) {
				rescheduled := j.Reschedule(time.Now())
				keepieotel.JobRescheduled(ctx)
				slog.Info("rescheduling retryable job",
					slog.String("job_id", j.JobID()),
					slog.String("error", err.Error()),
				)
				if sendErr := producer.Send(ctx, rescheduled); sendErr != nil {
					slog.Error("rescheduling job",
						slog.String("job_id", j.JobID()),
						slog.String("error", sendErr.Error()),
					)
				}
				return
			}

			keepieotel.JobFailed(ctx)
			slog.Error("handling job",
				slog.String("job_id", j.JobID()),
				slog.String("error", err.Error()),
			)
			return
		}

		keepieotel.RecordProcessingDuration(ctx, time.Since(start))
		keepieotel.JobProcessed(ctx)
		slog.Debug("job processed successfully",
			slog.String("job_id", j.JobID()),
		)
	}

	workerPool := pool.New(cfg.poolSize, wrappedHandler)

	consumer, err := kafka.NewConsumer(cfg.brokers, cfg.groupID, cfg.topic, workerPool, tlsCfg)
	if err != nil {
		slog.Error("creating consumer", slog.String("error", err.Error()))
		os.Exit(1)
	}

	go func() {
		slog.Info("starting consumer",
			slog.Int("pool_size", cfg.poolSize),
		)
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

	consumer.Close()
	workerPool.Close()
	producer.Close()
}
