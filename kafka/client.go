package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/matmazurk/oidc-keepie/job"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating sync producer: %w", err)
	}
	return &Producer{producer: producer}, nil
}

func (p *Producer) Send(ctx context.Context, topic string, j job.Job) error {
	data, err := json.Marshal(toKafkaJob(j))
	if err != nil {
		return fmt.Errorf("marshaling job: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("sending message: %w", err)
	}
	keepieotel.JobProduced(ctx, topic)
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

type Handler func(ctx context.Context, j job.Job) error

type Consumer struct {
	group    sarama.ConsumerGroup
	producer *Producer
	handler  Handler
}

func NewConsumer(brokers []string, groupID string, producer *Producer, handler Handler) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	return &Consumer{
		group:    group,
		producer: producer,
		handler:  handler,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, topic string) error {
	gh := &groupHandler{
		handler:  c.handler,
		producer: c.producer,
		topic:    topic,
	}

	for {
		err := c.group.Consume(ctx, []string{topic}, gh)
		if err != nil {
			return fmt.Errorf("consuming: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) Close() error {
	return c.group.Close()
}

type groupHandler struct {
	handler  Handler
	producer *Producer
	topic    string
}

func (h *groupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		ctx := session.Context()
		session.MarkMessage(msg, "")
		session.Commit()

		var kj kafkaJob
		if err := json.Unmarshal(msg.Value, &kj); err != nil {
			slog.Error("unmarshaling message", slog.String("error", err.Error()))
			keepieotel.UnmarshalError(ctx, h.topic)
			continue
		}

		j, err := kj.toJob()
		if err != nil {
			slog.Error("converting kafka job to domain job", slog.String("error", err.Error()))
			keepieotel.UnmarshalError(ctx, h.topic)
			continue
		}

		keepieotel.JobConsumed(ctx, h.topic)
		if !j.RescheduledAt().IsZero() {
			keepieotel.RecordTimeInQueue(ctx, h.topic, time.Since(j.RescheduledAt()))
		} else {
			keepieotel.RecordTimeInQueue(ctx, h.topic, time.Since(j.CreatedAt()))
		}

		start := time.Now()
		if err := h.handler(ctx, j); err != nil {
			keepieotel.RecordProcessingDuration(ctx, h.topic, time.Since(start))
			if job.IsRetryable(err) {
				rescheduled := j.Reschedule(time.Now())
				slog.Info("rescheduling retryable job",
					slog.String("job_id", j.JobID()),
					slog.Int("retry_count", rescheduled.RetryCount()),
					slog.String("error", err.Error()),
				)
				keepieotel.JobRescheduled(ctx, h.topic)
				if sendErr := h.producer.Send(ctx, h.topic, rescheduled); sendErr != nil {
					slog.Error("rescheduling job", slog.String("job_id", j.JobID()), slog.String("error", sendErr.Error()))
				}
				continue
			}
			keepieotel.JobFailed(ctx, h.topic)
			slog.Error("handling job", slog.String("job_id", j.JobID()), slog.String("error", err.Error()))
			continue
		}

		keepieotel.RecordProcessingDuration(ctx, h.topic, time.Since(start))
		keepieotel.JobProcessed(ctx, h.topic)
	}

	return nil
}
