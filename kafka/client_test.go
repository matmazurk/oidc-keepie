//go:build integration

package kafka_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matmazurk/oidc-keepie/job"
	kfk "github.com/matmazurk/oidc-keepie/kafka"
	"github.com/matmazurk/oidc-keepie/pool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers []string
	tlsCfg  *tls.Config
)

func TestMain(m *testing.M) {
	brokersEnv := os.Getenv("KAFKA_TEST_BROKERS")
	if brokersEnv == "" {
		// Not configured — tests will skip via requireKafka.
		os.Exit(m.Run())
	}
	brokers = strings.Split(brokersEnv, ",")

	caFile := os.Getenv("KAFKA_TEST_CA_FILE")
	certFile := os.Getenv("KAFKA_TEST_CERT_FILE")
	keyFile := os.Getenv("KAFKA_TEST_KEY_FILE")
	if caFile == "" || certFile == "" || keyFile == "" {
		panic("KAFKA_TEST_BROKERS is set but KAFKA_TEST_CA_FILE/KAFKA_TEST_CERT_FILE/KAFKA_TEST_KEY_FILE are not all set")
	}

	var err error
	tlsCfg, err = kfk.LoadTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		panic(fmt.Sprintf("loading kafka test tls config: %v", err))
	}

	os.Exit(m.Run())
}

func requireKafka(t *testing.T) {
	t.Helper()
	if len(brokers) == 0 {
		t.Skip("KAFKA_TEST_BROKERS not set; skipping kafka integration test")
	}
}

func createTopic(t *testing.T, topic string, partitions int) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsCfg),
	)
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	_, err = admin.CreateTopic(context.Background(), int32(partitions), 1, nil, topic)
	if err != nil {
		t.Fatalf("creating topic %s: %v", topic, err)
	}
}

func TestProduceAndConsume(t *testing.T) {
	requireKafka(t)
	topic := fmt.Sprintf("test-produce-consume-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)
	j := job.MustNew("job-1", "https://example.com/webhook", now)

	if err := producer.Send(ctx, j); err != nil {
		t.Fatalf("sending message: %v", err)
	}

	received := make(chan job.Job, 1)
	p := pool.New(2, func(ctx context.Context, j job.Job) {
		received <- j
	})
	defer p.Close()

	consumer, err := kfk.NewConsumer(brokers, "test-group-produce-consume", topic, p, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Start(ctx); err != nil {
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
	requireKafka(t)
	topic := fmt.Sprintf("test-workload-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
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

	makeHandler := func(name string) func(context.Context, job.Job) {
		return func(ctx context.Context, j job.Job) {
			mu.Lock()
			defer mu.Unlock()
			receivedByConsumer[name] = append(receivedByConsumer[name], j.JobID())
			total := len(receivedByConsumer["consumer-1"]) + len(receivedByConsumer["consumer-2"])
			if total == messageCount {
				close(allReceived)
			}
		}
	}

	groupID := fmt.Sprintf("test-group-workload-%d", time.Now().UnixNano())

	p1 := pool.New(2, makeHandler("consumer-1"))
	defer p1.Close()
	consumer1, err := kfk.NewConsumer(brokers, groupID, topic, p1, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}
	defer consumer1.Close()

	p2 := pool.New(2, makeHandler("consumer-2"))
	defer p2.Close()
	consumer2, err := kfk.NewConsumer(brokers, groupID, topic, p2, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	go func() {
		if err := consumer1.Start(ctx); err != nil {
			t.Logf("consumer 1 stopped: %v", err)
		}
	}()
	go func() {
		if err := consumer2.Start(ctx); err != nil {
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
		if err := producer.Send(ctx, j); err != nil {
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
	requireKafka(t)
	topic := fmt.Sprintf("test-rebalance-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
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

	p1 := pool.New(2, func(ctx context.Context, j job.Job) {
		mu.Lock()
		consumer1Messages = append(consumer1Messages, j.JobID())
		mu.Unlock()
		consumer1Received <- j.JobID()
	})
	defer p1.Close()

	consumer1, err := kfk.NewConsumer(brokers, groupID, topic, p1, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}

	p2 := pool.New(2, func(ctx context.Context, j job.Job) {
		mu.Lock()
		consumer2Messages = append(consumer2Messages, j.JobID())
		mu.Unlock()
		consumer2Received <- j.JobID()
	})
	defer p2.Close()

	consumer2, err := kfk.NewConsumer(brokers, groupID, topic, p2, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	// consumer 1 gets its own context so we can stop it mid-test
	ctx1, cancel1 := context.WithCancel(ctx)

	go func() {
		if err := consumer1.Start(ctx1); err != nil {
			t.Logf("consumer 1 stopped: %v", err)
		}
	}()
	go func() {
		if err := consumer2.Start(ctx); err != nil {
			t.Logf("consumer 2 stopped: %v", err)
		}
	}()

	// wait for both consumers to join
	time.Sleep(5 * time.Second)

	// send messages while both consumers are active
	for i := range 5 {
		j := job.MustNew(fmt.Sprintf("job-phase1-%d", i), "https://example.com/webhook", now)
		if err := producer.Send(ctx, j); err != nil {
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
		if err := producer.Send(ctx, j); err != nil {
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
	requireKafka(t)
	topic := fmt.Sprintf("test-offset-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)
	groupID := fmt.Sprintf("test-group-offset-%d", time.Now().UnixNano())

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)

	// send 3 messages
	for i := range 3 {
		j := job.MustNew(fmt.Sprintf("job-%d", i), "https://example.com/webhook", now)
		if err := producer.Send(ctx, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

	// consume all 3 messages with first consumer
	received1 := make(chan string, 10)
	p1 := pool.New(2, func(ctx context.Context, j job.Job) {
		received1 <- j.JobID()
	})
	defer p1.Close()

	consumer1, err := kfk.NewConsumer(brokers, groupID, topic, p1, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(ctx)

	go func() {
		if err := consumer1.Start(ctx1); err != nil {
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
		if err := producer.Send(ctx, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

	// start a new consumer in the same group — should only get the 2 new messages
	received2 := make(chan string, 10)
	p2 := pool.New(2, func(ctx context.Context, j job.Job) {
		received2 <- j.JobID()
	})
	defer p2.Close()

	consumer2, err := kfk.NewConsumer(brokers, groupID, topic, p2, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	go func() {
		if err := consumer2.Start(ctx); err != nil {
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
	requireKafka(t)
	topic := fmt.Sprintf("test-retryable-%d", time.Now().UnixNano())
	createTopic(t, topic, 6)
	groupID := fmt.Sprintf("test-group-retryable-%d", time.Now().UnixNano())

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
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

	p := pool.New(2, func(ctx context.Context, j job.Job) {
		mu.Lock()
		attempts[j.JobID()]++
		attempt := attempts[j.JobID()]
		mu.Unlock()

		if attempt == 1 {
			rescheduled := j.Reschedule(time.Now())
			if sendErr := producer.Send(ctx, rescheduled); sendErr != nil {
				t.Logf("rescheduling failed: %v", sendErr)
			}
			return
		}

		mu.Lock()
		retriedJob = j
		mu.Unlock()
		close(done)
	})
	defer p.Close()

	consumer, err := kfk.NewConsumer(brokers, groupID, topic, p, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Start(ctx); err != nil {
			t.Logf("consumer stopped: %v", err)
		}
	}()

	j := job.MustNew("job-1", "https://example.com/webhook", now)
	if err := producer.Send(ctx, j); err != nil {
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
