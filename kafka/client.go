package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/matmazurk/oidc-keepie/job"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating producer client: %w", err)
	}
	return &Producer{client: client, topic: topic}, nil
}

func (p *Producer) Send(ctx context.Context, j job.Job) error {
	ctx, span := keepieotel.Tracer().Start(ctx, "kafka.produce")
	defer span.End()

	data, err := json.Marshal(toKafkaJob(j))
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshaling job: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Value: data,
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("sending message: %w", err)
	}
	keepieotel.JobScheduled(ctx)
	slog.Debug("scheduled job", slog.String("job_id", j.JobID()))

	return nil
}

func (p *Producer) Close() {
	p.client.Close()
}

type Handler func(ctx context.Context, j job.Job) error

type Consumer struct {
	client   *kgo.Client
	producer *Producer
	handler  Handler
}

func NewConsumer(brokers []string, groupID, topic string, producer *Producer, handler Handler) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating consumer client: %w", err)
	}

	return &Consumer{
		client:   client,
		producer: producer,
		handler:  handler,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for _, e := range fetches.Errors() {
			if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, kgo.ErrClientClosed) {
				continue
			}
			slog.Error("fetch error",
				slog.Int("partition", int(e.Partition)),
				slog.String("error", e.Err.Error()),
			)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			c.processRecord(ctx, record)
		})
	}
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) {
	ctx, span := keepieotel.Tracer().Start(ctx, "kafka.process")
	defer span.End()

	slog.Debug("received record",
		slog.Int("partition", int(record.Partition)),
		slog.Int64("offset", record.Offset),
	)

	received := time.Now()

	if err := c.client.CommitRecords(ctx, record); err != nil {
		slog.Error("committing offset",
			slog.String("error", err.Error()),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset),
		)
		keepieotel.OffsetCommitError(ctx)
		return
	}

	var kj kafkaJob
	if err := json.Unmarshal(record.Value, &kj); err != nil {
		slog.Error("unmarshaling message", slog.String("error", err.Error()))
		keepieotel.UnmarshalError(ctx)
		return
	}

	j, err := kj.toJob()
	if err != nil {
		slog.Error("converting kafka job to domain job", slog.String("error", err.Error()))
		keepieotel.UnmarshalError(ctx)
		return
	}

	keepieotel.JobConsumed(ctx)
	if !j.RescheduledAt().IsZero() {
		keepieotel.RecordTimeInQueue(ctx, received.Sub(j.RescheduledAt()))
	} else {
		keepieotel.RecordTimeInQueue(ctx, received.Sub(j.CreatedAt()))
	}

	slog.Debug("processing job",
		slog.String("job_id", j.JobID()),
		slog.Int("partition", int(record.Partition)),
		slog.Int64("offset", record.Offset),
		slog.Int("retry_count", j.RetryCount()),
	)

	start := time.Now()
	if err := c.handler(ctx, j); err != nil {
		keepieotel.RecordProcessingDuration(ctx, time.Since(start))

		if job.IsRetryable(err) {
			rescheduled := j.Reschedule(time.Now())
			keepieotel.JobRescheduled(ctx)

			slog.Info("rescheduling retryable job",
				slog.Any("job", rescheduled),
				slog.String("error", err.Error()),
			)

			if sendErr := c.producer.Send(ctx, rescheduled); sendErr != nil {
				slog.Error("rescheduling job", slog.String("job_id", j.JobID()), slog.String("error", sendErr.Error()))
			}

			return
		}
		keepieotel.JobFailed(ctx)
		slog.Error("handling job", slog.String("job_id", j.JobID()), slog.String("error", err.Error()))
		return
	}

	keepieotel.RecordProcessingDuration(ctx, time.Since(start))
	keepieotel.JobProcessed(ctx)

	slog.Debug("job processed successfully",
		slog.String("job_id", j.JobID()),
		slog.Duration("duration", time.Since(start)),
	)
}

func (c *Consumer) Close() {
	c.client.Close()
}
