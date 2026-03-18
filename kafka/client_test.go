//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/matmazurk/oidc-keepie/job"
	kfk "github.com/matmazurk/oidc-keepie/kafka"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var brokers []string

func TestMain(m *testing.M) {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		panic(fmt.Sprintf("starting kafka container: %v", err))
	}
	defer tc.TerminateContainer(kafkaContainer)

	brokers, err = kafkaContainer.Brokers(ctx)
	if err != nil {
		panic(fmt.Sprintf("getting brokers: %v", err))
	}

	m.Run()
}

func createTopic(t *testing.T, topic string, partitions int) {
	t.Helper()

	cfg := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Fatalf("creating cluster admin: %v", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		t.Fatalf("creating topic %s: %v", topic, err)
	}
}

func TestProduceAndConsume(t *testing.T) {
	topic := fmt.Sprintf("test-produce-consume-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)

	producer, err := kfk.NewProducer(brokers)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)
	j := job.MustNew("job-1", "https://example.com/webhook", now)

	if err := producer.Send(ctx, topic, j); err != nil {
		t.Fatalf("sending message: %v", err)
	}

	received := make(chan job.Job, 1)
	consumer, err := kfk.NewConsumer(brokers, "test-group-produce-consume", producer, func(ctx context.Context, j job.Job) error {
		received <- j
		return nil
	})
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Start(ctx, topic); err != nil {
			t.Logf("consumer stopped: %v", err)
		}
	}()

	select {
	case got := <-received:
		if got.JobID() != "job-1" {
			t.Errorf("expected JobID job-1, got %s", got.JobID())
		}
		if got.WebhookURL() != "https://example.com/webhook" {
			t.Errorf("expected WebhookURL https://example.com/webhook, got %s", got.WebhookURL())
		}
		if !got.CreatedAt().Equal(now) {
			t.Errorf("expected CreatedAt %v, got %v", now, got.CreatedAt())
		}
		if got.RetryCount() != 0 {
			t.Errorf("expected RetryCount 0, got %d", got.RetryCount())
		}
		if !got.RescheduledAt().IsZero() {
			t.Errorf("expected RescheduledAt to be zero, got %v", got.RescheduledAt())
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestWorkloadDistribution(t *testing.T) {
	topic := fmt.Sprintf("test-workload-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)

	producer, err := kfk.NewProducer(brokers)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	messageCount := 200
	now := time.Now().Truncate(time.Second)

	var mu sync.Mutex
	receivedByConsumer := map[string][]string{
		"consumer-1": {},
		"consumer-2": {},
	}
	allReceived := make(chan struct{})

	makeHandler := func(name string) kfk.Handler {
		return func(ctx context.Context, j job.Job) error {
			mu.Lock()
			defer mu.Unlock()
			receivedByConsumer[name] = append(receivedByConsumer[name], j.JobID())
			total := len(receivedByConsumer["consumer-1"]) + len(receivedByConsumer["consumer-2"])
			if total == messageCount {
				close(allReceived)
			}
			return nil
		}
	}

	groupID := fmt.Sprintf("test-group-workload-%d", time.Now().UnixNano())

	consumer1, err := kfk.NewConsumer(brokers, groupID, producer, makeHandler("consumer-1"))
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}
	defer consumer1.Close()
	consumer2, err := kfk.NewConsumer(brokers, groupID, producer, makeHandler("consumer-2"))
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	go func() {
		if err := consumer1.Start(ctx, topic); err != nil {
			t.Logf("consumer 1 stopped: %v", err)
		}
	}()
	go func() {
		if err := consumer2.Start(ctx, topic); err != nil {
			t.Logf("consumer 2 stopped: %v", err)
		}
	}()

	// wait for consumers to join the group before producing
	time.Sleep(5 * time.Second)

	for i := range messageCount {
		j := job.MustNew(
			fmt.Sprintf("job-%d", i),
			"https://example.com/webhook",
			now,
		)
		if err := producer.Send(ctx, topic, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

	select {
	case <-allReceived:
	case <-ctx.Done():
		t.Fatal("timed out waiting for all messages")
	}

	mu.Lock()
	defer mu.Unlock()

	total := len(receivedByConsumer["consumer-1"]) + len(receivedByConsumer["consumer-2"])
	if total != messageCount {
		t.Errorf("expected %d total messages, got %d", messageCount, total)
	}

	if len(receivedByConsumer["consumer-1"]) == 0 || len(receivedByConsumer["consumer-2"]) == 0 {
		t.Errorf("expected both consumers to receive messages, got consumer-1=%d consumer-2=%d",
			len(receivedByConsumer["consumer-1"]),
			len(receivedByConsumer["consumer-2"]),
		)
	}

	// check no duplicates
	seen := make(map[string]bool)
	for _, id := range receivedByConsumer["consumer-1"] {
		if seen[id] {
			t.Errorf("duplicate message: %s", id)
		}
		seen[id] = true
	}
	for _, id := range receivedByConsumer["consumer-2"] {
		if seen[id] {
			t.Errorf("duplicate message: %s", id)
		}
		seen[id] = true
	}

	t.Logf("consumer-1 received %d messages, consumer-2 received %d messages",
		len(receivedByConsumer["consumer-1"]),
		len(receivedByConsumer["consumer-2"]),
	)
}

func TestConsumerRebalancing(t *testing.T) {
	topic := fmt.Sprintf("test-rebalance-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)

	producer, err := kfk.NewProducer(brokers)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)
	groupID := fmt.Sprintf("test-group-rebalance-%d", time.Now().UnixNano())

	var mu sync.Mutex
	consumer1Messages := []string{}
	consumer2Messages := []string{}

	consumer1Received := make(chan string, 100)
	consumer2Received := make(chan string, 100)

	consumer1, err := kfk.NewConsumer(brokers, groupID, producer, func(ctx context.Context, j job.Job) error {
		mu.Lock()
		consumer1Messages = append(consumer1Messages, j.JobID())
		mu.Unlock()
		consumer1Received <- j.JobID()
		return nil
	})
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}

	consumer2, err := kfk.NewConsumer(brokers, groupID, producer, func(ctx context.Context, j job.Job) error {
		mu.Lock()
		consumer2Messages = append(consumer2Messages, j.JobID())
		mu.Unlock()
		consumer2Received <- j.JobID()
		return nil
	})
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	// consumer 1 gets its own context so we can stop it mid-test
	ctx1, cancel1 := context.WithCancel(ctx)

	go func() {
		if err := consumer1.Start(ctx1, topic); err != nil {
			t.Logf("consumer 1 stopped: %v", err)
		}
	}()
	go func() {
		if err := consumer2.Start(ctx, topic); err != nil {
			t.Logf("consumer 2 stopped: %v", err)
		}
	}()

	// wait for both consumers to join
	time.Sleep(5 * time.Second)

	// send messages while both consumers are active
	for i := range 5 {
		j := job.MustNew(fmt.Sprintf("job-phase1-%d", i), "https://example.com/webhook", now)
		if err := producer.Send(ctx, topic, j); err != nil {
			t.Fatalf("sending phase 1 message %d: %v", i, err)
		}
	}

	// wait for phase 1 messages to be consumed
	phase1Count := 0
	for phase1Count < 5 {
		select {
		case <-consumer1Received:
			phase1Count++
		case <-consumer2Received:
			phase1Count++
		case <-ctx.Done():
			t.Fatal("timed out waiting for phase 1 messages")
		}
	}

	// stop consumer 1
	cancel1()
	consumer1.Close()

	// wait for rebalance
	time.Sleep(5 * time.Second)

	// send more messages — consumer 2 should get all of them
	for i := range 5 {
		j := job.MustNew(fmt.Sprintf("job-phase2-%d", i), "https://example.com/webhook", now)
		if err := producer.Send(ctx, topic, j); err != nil {
			t.Fatalf("sending phase 2 message %d: %v", i, err)
		}
	}

	phase2Count := 0
	for phase2Count < 5 {
		select {
		case <-consumer2Received:
			phase2Count++
		case <-ctx.Done():
			t.Fatal("timed out waiting for phase 2 messages")
		}
	}

	mu.Lock()
	defer mu.Unlock()

	t.Logf("consumer-1 received %d messages total", len(consumer1Messages))
	t.Logf("consumer-2 received %d messages total", len(consumer2Messages))

	if len(consumer2Messages) < 5 {
		t.Errorf("expected consumer-2 to receive at least 5 messages (all phase 2), got %d", len(consumer2Messages))
	}
}

func TestOffsetPersistence(t *testing.T) {
	topic := fmt.Sprintf("test-offset-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)
	groupID := fmt.Sprintf("test-group-offset-%d", time.Now().UnixNano())

	producer, err := kfk.NewProducer(brokers)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)

	// send 3 messages
	for i := range 3 {
		j := job.MustNew(fmt.Sprintf("job-%d", i), "https://example.com/webhook", now)
		if err := producer.Send(ctx, topic, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

	// consume all 3 messages with first consumer
	received1 := make(chan string, 10)
	consumer1, err := kfk.NewConsumer(brokers, groupID, producer, func(ctx context.Context, j job.Job) error {
		received1 <- j.JobID()
		return nil
	})
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(ctx)

	go func() {
		if err := consumer1.Start(ctx1, topic); err != nil {
			t.Logf("consumer 1 stopped: %v", err)
		}
	}()

	count := 0
	for count < 3 {
		select {
		case <-received1:
			count++
		case <-ctx.Done():
			t.Fatal("timed out waiting for first consumer to receive messages")
		}
	}

	// stop first consumer
	cancel1()
	consumer1.Close()

	// send 2 more messages
	for i := 3; i < 5; i++ {
		j := job.MustNew(fmt.Sprintf("job-%d", i), "https://example.com/webhook", now)
		if err := producer.Send(ctx, topic, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

	// start a new consumer in the same group — should only get the 2 new messages
	received2 := make(chan string, 10)
	consumer2, err := kfk.NewConsumer(brokers, groupID, producer, func(ctx context.Context, j job.Job) error {
		received2 <- j.JobID()
		return nil
	})
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	go func() {
		if err := consumer2.Start(ctx, topic); err != nil {
			t.Logf("consumer 2 stopped: %v", err)
		}
	}()

	var newMessages []string
	count = 0
	for count < 2 {
		select {
		case id := <-received2:
			newMessages = append(newMessages, id)
			count++
		case <-ctx.Done():
			t.Fatal("timed out waiting for second consumer to receive messages")
		}
	}

	// give a moment to check no extra messages arrive
	select {
	case id := <-received2:
		t.Errorf("unexpected extra message received: %s", id)
	case <-time.After(3 * time.Second):
		// good — no more messages
	}

	if len(newMessages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(newMessages))
	}
	t.Logf("second consumer received: %v", newMessages)
}

func TestRetryableError(t *testing.T) {
	topic := fmt.Sprintf("test-retryable-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)
	groupID := fmt.Sprintf("test-group-retryable-%d", time.Now().UnixNano())

	producer, err := kfk.NewProducer(brokers)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)

	var mu sync.Mutex
	attempts := map[string]int{}
	var retriedJob job.Job
	done := make(chan struct{})

	consumer, err := kfk.NewConsumer(brokers, groupID, producer, func(ctx context.Context, j job.Job) error {
		mu.Lock()
		attempts[j.JobID()]++
		attempt := attempts[j.JobID()]
		mu.Unlock()

		if attempt == 1 {
			return job.MakeRetryable(fmt.Errorf("temporary failure"))
		}

		mu.Lock()
		retriedJob = j
		mu.Unlock()
		close(done)
		return nil
	})
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Start(ctx, topic); err != nil {
			t.Logf("consumer stopped: %v", err)
		}
	}()

	j := job.MustNew("job-1", "https://example.com/webhook", now)
	if err := producer.Send(ctx, topic, j); err != nil {
		t.Fatalf("sending message: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for retried message")
	}

	mu.Lock()
	defer mu.Unlock()

	if attempts["job-1"] != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts["job-1"])
	}
	if retriedJob.JobID() != "job-1" {
		t.Errorf("expected retried job to keep jobID job-1, got %s", retriedJob.JobID())
	}
	if retriedJob.RetryCount() != 1 {
		t.Errorf("expected retry count 1, got %d", retriedJob.RetryCount())
	}
	if retriedJob.RescheduledAt().IsZero() {
		t.Error("expected rescheduledAt to be set")
	}
	if !retriedJob.CreatedAt().Equal(now) {
		t.Errorf("expected createdAt to be preserved, got %v", retriedJob.CreatedAt())
	}
}
