package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/matmazurk/oidc-keepie/job"
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
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

type Handler func(ctx context.Context, j job.Job) error

type Consumer struct {
	group   sarama.ConsumerGroup
	handler Handler
}

func NewConsumer(brokers []string, groupID string, handler Handler) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	return &Consumer{
		group:   group,
		handler: handler,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, topic string) error {
	gh := &groupHandler{
		handler: c.handler,
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
	handler Handler
}

func (h *groupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
		session.Commit()

		var kj kafkaJob
		if err := json.Unmarshal(msg.Value, &kj); err != nil {
			log.Printf("unmarshaling message: %v", err)
			continue
		}

		j, err := kj.toJob()
		if err != nil {
			log.Printf("converting kafka job to domain job: %v", err)
			continue
		}

		if err := h.handler(session.Context(), j); err != nil {
			log.Printf("handling job %s: %v", j.ID(), err)
		}
	}
	return nil
}
