package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	data, err := json.Marshal(toKafkaJob(j))
	if err != nil {
		return fmt.Errorf("marshaling job: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Value: data,
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("sending message: %w", err)
	}
	keepieotel.JobProduced(ctx, p.topic)
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
	topic    string
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
		topic:    topic,
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
				slog.String("topic", e.Topic),
				slog.Int("partition", int(e.Partition)),
				slog.String("error", e.Err.Error()),
			)
		}

		var wg sync.WaitGroup
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			wg.Go(func() {
				for _, record := range p.Records {
					c.processRecord(ctx, record)
				}
			})
		})
		wg.Wait()
	}
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) {
	if err := c.client.CommitRecords(ctx, record); err != nil {
		slog.Error("committing offset",
			slog.String("error", err.Error()),
			slog.String("topic", record.Topic),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset),
		)
		keepieotel.OffsetCommitError(ctx, c.topic)
		return
	}

	var kj kafkaJob
	if err := json.Unmarshal(record.Value, &kj); err != nil {
		slog.Error("unmarshaling message", slog.String("error", err.Error()))
		keepieotel.UnmarshalError(ctx, c.topic)
		return
	}

	j, err := kj.toJob()
	if err != nil {
		slog.Error("converting kafka job to domain job", slog.String("error", err.Error()))
		keepieotel.UnmarshalError(ctx, c.topic)
		return
	}

	keepieotel.JobConsumed(ctx, c.topic)
	if !j.RescheduledAt().IsZero() {
		keepieotel.RecordTimeInQueue(ctx, c.topic, time.Since(j.RescheduledAt()))
	} else {
		keepieotel.RecordTimeInQueue(ctx, c.topic, time.Since(j.CreatedAt()))
	}

	start := time.Now()
	if err := c.handler(ctx, j); err != nil {
		keepieotel.RecordProcessingDuration(ctx, c.topic, time.Since(start))
		if job.IsRetryable(err) {
			rescheduled := j.Reschedule(time.Now())
			slog.Info("rescheduling retryable job",
				slog.String("job_id", j.JobID()),
				slog.Int("retry_count", rescheduled.RetryCount()),
				slog.String("error", err.Error()),
			)
			keepieotel.JobRescheduled(ctx, c.topic)
			if sendErr := c.producer.Send(ctx, rescheduled); sendErr != nil {
				slog.Error("rescheduling job", slog.String("job_id", j.JobID()), slog.String("error", sendErr.Error()))
			}
			return
		}
		keepieotel.JobFailed(ctx, c.topic)
		slog.Error("handling job", slog.String("job_id", j.JobID()), slog.String("error", err.Error()))
		return
	}

	keepieotel.RecordProcessingDuration(ctx, c.topic, time.Since(start))
	keepieotel.JobProcessed(ctx, c.topic)
}

func (c *Consumer) Close() {
	c.client.Close()
}
