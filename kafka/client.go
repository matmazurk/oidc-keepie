package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/matmazurk/oidc-keepie/job"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
	"github.com/matmazurk/oidc-keepie/pool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string, tlsCfg *tls.Config) (*Producer, error) {
	if tlsCfg == nil {
		return nil, fmt.Errorf("tls config is required")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsCfg),
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

type Submitter interface {
	Submit(ctx context.Context, j pool.Job) error
}

type Consumer struct {
	client    *kgo.Client
	submitter Submitter
}

func NewConsumer(brokers []string, groupID, topic string, submitter Submitter, tlsCfg *tls.Config, extraOpts ...kgo.Opt) (*Consumer, error) {
	if tlsCfg == nil {
		return nil, fmt.Errorf("tls config is required")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsCfg),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	opts = append(opts, extraOpts...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating consumer client: %w", err)
	}

	return &Consumer{
		client:    client,
		submitter: submitter,
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

	// unmarshal before submit — poison pill prevention
	var kj kafkaJob
	if err := json.Unmarshal(record.Value, &kj); err != nil {
		slog.Error("unmarshaling message", slog.String("error", err.Error()))
		keepieotel.UnmarshalError(ctx)
		if commitErr := c.client.CommitRecords(ctx, record); commitErr != nil {
			slog.Error("committing poison pill offset", slog.String("error", commitErr.Error()))
		}
		return
	}

	j, err := kj.toJob()
	if err != nil {
		slog.Error("converting kafka job to domain job", slog.String("error", err.Error()))
		keepieotel.UnmarshalError(ctx)
		if commitErr := c.client.CommitRecords(ctx, record); commitErr != nil {
			slog.Error("committing bad job offset", slog.String("error", commitErr.Error()))
		}
		return
	}

	keepieotel.JobConsumed(ctx)
	if !j.RescheduledAt().IsZero() {
		keepieotel.RecordTimeInQueue(ctx, received.Sub(j.RescheduledAt()))
	} else {
		keepieotel.RecordTimeInQueue(ctx, received.Sub(j.CreatedAt()))
	}

	slog.Debug("submitting job to pool",
		slog.String("job_id", j.JobID()),
		slog.Int("partition", int(record.Partition)),
		slog.Int64("offset", record.Offset),
		slog.Int("retry_count", j.RetryCount()),
	)

	commitFn := func() error {
		if err := c.client.CommitRecords(ctx, record); err != nil {
			keepieotel.OffsetCommitError(ctx)
			return err
		}
		return nil
	}

	if err := c.submitter.Submit(ctx, pool.Job{
		Payload: j,
		Commit:  commitFn,
	}); err != nil {
		slog.Error("submitting job to pool",
			slog.String("job_id", j.JobID()),
			slog.String("error", err.Error()),
		)
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
