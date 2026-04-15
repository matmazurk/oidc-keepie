//go:build integration

package kafka_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matmazurk/oidc-keepie/job"
	kfk "github.com/matmazurk/oidc-keepie/kafka"
	"github.com/matmazurk/oidc-keepie/pool"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers     []string
	tlsCfg      *tls.Config
	topic       string
	groupPrefix string
	partitions  int
	runID       string
)

func TestMain(m *testing.M) {
	brokersEnv := os.Getenv("KAFKA_TEST_BROKERS")
	if brokersEnv == "" {
		// Not configured — tests will skip via requireKafka.
		os.Exit(m.Run())
	}
	brokers = strings.Split(brokersEnv, ",")

	topic = os.Getenv("KAFKA_TEST_TOPIC")
	if topic == "" {
		panic("KAFKA_TEST_BROKERS is set but KAFKA_TEST_TOPIC is not set")
	}

	caFile := os.Getenv("KAFKA_TEST_CA_FILE")
	certFile := os.Getenv("KAFKA_TEST_CERT_FILE")
	keyFile := os.Getenv("KAFKA_TEST_KEY_FILE")
	if caFile == "" || certFile == "" || keyFile == "" {
		panic("KAFKA_TEST_BROKERS is set but KAFKA_TEST_CA_FILE/KAFKA_TEST_CERT_FILE/KAFKA_TEST_KEY_FILE are not all set")
	}

	groupPrefix = os.Getenv("KAFKA_TEST_GROUP_PREFIX")

	partitions = 3
	if v := os.Getenv("KAFKA_TEST_PARTITIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			panic(fmt.Sprintf("invalid KAFKA_TEST_PARTITIONS=%q", v))
		}
		partitions = n
	}

	var err error
	tlsCfg, err = kfk.LoadTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		panic(fmt.Sprintf("loading kafka test tls config: %v", err))
	}

	runID = fmt.Sprintf("run-%d", time.Now().UnixNano())

	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	os.Exit(m.Run())
}

func requireKafka(t *testing.T) {
	t.Helper()
	if len(brokers) == 0 {
		t.Skip("KAFKA_TEST_BROKERS not set; skipping kafka integration test")
	}
}

var invocationSeq atomic.Int64

// newTestID returns the per-invocation tag embedded in every message this
// test produces. Includes runID plus a sequence counter so that repeated
// invocations within the same binary (e.g. -count=10) get distinct tags.
func newTestID(t *testing.T) string {
	t.Helper()
	n := invocationSeq.Add(1)
	return fmt.Sprintf("%s-%d-%s", runID, n, t.Name())
}

// newGroupID returns the consumer group name for the test — stable across
// runs so Kafka ACLs can be granted per-group under the required prefix.
func newGroupID(t *testing.T) string {
	t.Helper()
	return groupPrefix + t.Name()
}

func taggedJobID(testID, base string) string {
	return testID + ":" + base
}

func stripTag(testID, jobID string) string {
	return strings.TrimPrefix(jobID, testID+":")
}

// filteringHandler wraps a job handler so that jobs not tagged for this
// test's run are silently dropped before reaching the real handler.
func filteringHandler(testID string, inner func(context.Context, job.Job)) func(context.Context, job.Job) {
	prefix := testID + ":"
	return func(ctx context.Context, j job.Job) {
		if !strings.HasPrefix(j.JobID(), prefix) {
			return
		}
		inner(ctx, j)
	}
}

const syncMarker = "__sync__"

func isSyncJobID(jobID string) bool {
	return strings.Contains(jobID, ":"+syncMarker)
}

// syncingHandler wraps an inner handler so sync-marker jobs produced by
// waitForSync are absorbed and delivered to syncCh instead of inner.
// Callers should size syncCh to at least `partitions` to avoid dropping.
func syncingHandler(testID string, syncCh chan<- struct{}, inner func(context.Context, job.Job)) func(context.Context, job.Job) {
	return filteringHandler(testID, func(ctx context.Context, j job.Job) {
		if isSyncJobID(j.JobID()) {
			select {
			case syncCh <- struct{}{}:
			default:
			}
			return
		}
		inner(ctx, j)
	})
}

// waitForSync produces sync markers until each provided sync channel
// receives at least one signal. Markers are re-sent every 2 seconds to
// cover the window where consumers are still joining the group or
// rebalancing — a marker produced before a consumer was assigned its
// partition gets consumed by whichever consumer held that partition
// at the time, which may not be the one we're waiting on.
func waitForSync(t *testing.T, ctx context.Context, producer *kfk.Producer, testID string, syncChs ...<-chan struct{}) {
	t.Helper()

	sendMarkers := func() {
		for i := 0; i < partitions; i++ {
			j := job.MustNew(
				taggedJobID(testID, fmt.Sprintf("%s%d", syncMarker, i)),
				"https://example.com/sync",
				time.Now(),
			)
			if err := producer.Send(ctx, j); err != nil {
				t.Fatalf("sending sync marker %d: %v", i, err)
			}
		}
	}

	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	sendMarkers()
	for i, ch := range syncChs {
	wait:
		for {
			select {
			case <-ch:
				break wait
			case <-ticker.C:
				sendMarkers()
			case <-deadline:
				t.Fatalf("consumer %d did not sync within 30s", i)
			}
		}
	}
}

func TestProduceAndConsume(t *testing.T) {
	requireKafka(t)
	t.Parallel()
	testID := newTestID(t)
	groupID := newGroupID(t)

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)
	j := job.MustNew(taggedJobID(testID, "job-1"), "https://example.com/webhook", now)

	received := make(chan job.Job, 1)
	syncCh := make(chan struct{}, partitions)
	p := pool.New(2, syncingHandler(testID, syncCh, func(ctx context.Context, j job.Job) {
		received <- j
	}))
	defer p.Close()

	consumer, err := kfk.NewConsumer(brokers, groupID, topic, p, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		_ = consumer.Start(ctx)
	}()

	waitForSync(t, ctx, producer, testID, syncCh)

	if err := producer.Send(ctx, j); err != nil {
		t.Fatalf("sending message: %v", err)
	}

	select {
	case got := <-received:
		if stripTag(testID, got.JobID()) != "job-1" {
			t.Errorf("expected JobID job-1, got %s", stripTag(testID, got.JobID()))
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

// TestWorkloadDistribution assumes the shared test topic has at least 2
// partitions. With only 1 partition, only one consumer in the group will
// receive messages and this test will (correctly) fail.
func TestWorkloadDistribution(t *testing.T) {
	requireKafka(t)
	t.Parallel()
	testID := newTestID(t)
	groupID := newGroupID(t)

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
	var closedOnce sync.Once

	makeHandler := func(name string) func(context.Context, job.Job) {
		return func(ctx context.Context, j job.Job) {
			mu.Lock()
			defer mu.Unlock()
			receivedByConsumer[name] = append(receivedByConsumer[name], stripTag(testID, j.JobID()))
			total := len(receivedByConsumer["consumer-1"]) + len(receivedByConsumer["consumer-2"])
			if total == messageCount {
				closedOnce.Do(func() { close(allReceived) })
			}
		}
	}

	sync1 := make(chan struct{}, partitions)
	p1 := pool.New(2, syncingHandler(testID, sync1, makeHandler("consumer-1")))
	defer p1.Close()
	consumer1, err := kfk.NewConsumer(brokers, groupID, topic, p1, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}
	defer consumer1.Close()

	sync2 := make(chan struct{}, partitions)
	p2 := pool.New(2, syncingHandler(testID, sync2, makeHandler("consumer-2")))
	defer p2.Close()
	consumer2, err := kfk.NewConsumer(brokers, groupID, topic, p2, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	go func() {
		_ = consumer1.Start(ctx)
	}()
	go func() {
		_ = consumer2.Start(ctx)
	}()

	waitForSync(t, ctx, producer, testID, sync1, sync2)

	for i := range messageCount {
		j := job.MustNew(
			taggedJobID(testID, fmt.Sprintf("job-%d", i)),
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
	t.Parallel()
	testID := newTestID(t)
	groupID := newGroupID(t)

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)

	var mu sync.Mutex
	consumer1Messages := []string{}
	consumer2Messages := []string{}

	consumer1Received := make(chan string, 100)
	consumer2Received := make(chan string, 100)

	sync1 := make(chan struct{}, partitions)
	p1 := pool.New(2, syncingHandler(testID, sync1, func(ctx context.Context, j job.Job) {
		id := stripTag(testID, j.JobID())
		mu.Lock()
		consumer1Messages = append(consumer1Messages, id)
		mu.Unlock()
		consumer1Received <- id
	}))
	defer p1.Close()

	consumer1, err := kfk.NewConsumer(brokers, groupID, topic, p1, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}

	sync2 := make(chan struct{}, partitions*2)
	p2 := pool.New(2, syncingHandler(testID, sync2, func(ctx context.Context, j job.Job) {
		id := stripTag(testID, j.JobID())
		mu.Lock()
		consumer2Messages = append(consumer2Messages, id)
		mu.Unlock()
		consumer2Received <- id
	}))
	defer p2.Close()

	consumer2, err := kfk.NewConsumer(brokers, groupID, topic, p2, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	// consumer 1 gets its own context so we can stop it mid-test
	ctx1, cancel1 := context.WithCancel(ctx)

	go func() {
		_ = consumer1.Start(ctx1)
	}()
	go func() {
		_ = consumer2.Start(ctx)
	}()

	// phase 1: both consumers active
	waitForSync(t, ctx, producer, testID, sync1, sync2)

	for i := range 5 {
		j := job.MustNew(taggedJobID(testID, fmt.Sprintf("job-phase1-%d", i)), "https://example.com/webhook", now)
		if err := producer.Send(ctx, j); err != nil {
			t.Fatalf("sending phase 1 message %d: %v", i, err)
		}
	}

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

	// stop consumer 1 and wait for consumer 2 to take over
	cancel1()
	consumer1.Close()

	// phase 2: only consumer 2 — re-sync to confirm rebalance completed
	waitForSync(t, ctx, producer, testID, sync2)

	for i := range 5 {
		j := job.MustNew(taggedJobID(testID, fmt.Sprintf("job-phase2-%d", i)), "https://example.com/webhook", now)
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
	t.Parallel()
	testID := newTestID(t)
	groupID := newGroupID(t)

	producer, err := kfk.NewProducer(brokers, topic, tlsCfg)
	if err != nil {
		t.Fatalf("creating producer: %v", err)
	}
	defer producer.Close()

	ctx := t.Context()

	now := time.Now().Truncate(time.Second)

	// consumer 1 — consumes first batch so offsets get committed for the group
	received1 := make(chan string, 10)
	sync1 := make(chan struct{}, partitions)
	p1 := pool.New(2, syncingHandler(testID, sync1, func(ctx context.Context, j job.Job) {
		received1 <- stripTag(testID, j.JobID())
	}))
	defer p1.Close()

	consumer1, err := kfk.NewConsumer(brokers, groupID, topic, p1, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer 1: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(ctx)
	go func() {
		_ = consumer1.Start(ctx1)
	}()

	waitForSync(t, ctx, producer, testID, sync1)

	for i := range 3 {
		j := job.MustNew(taggedJobID(testID, fmt.Sprintf("job-%d", i)), "https://example.com/webhook", now)
		if err := producer.Send(ctx, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

	count := 0
	for count < 3 {
		select {
		case <-received1:
			count++
		case <-ctx.Done():
			t.Fatal("timed out waiting for first consumer to receive messages")
		}
	}

	// stop consumer 1 — its committed offsets stay in the group
	cancel1()
	consumer1.Close()

	// Start a new consumer in the same group. It uses the default reset
	// (AtStart) so we prove committed offsets drive the resume, not the
	// reset policy. A sync marker confirms consumer 2 is caught up past
	// the first batch before we produce the second batch — this avoids
	// flakes from commits that raced with consumer 1's shutdown.
	received2 := make(chan string, 10)
	sync2 := make(chan struct{}, partitions)
	p2 := pool.New(2, syncingHandler(testID, sync2, func(ctx context.Context, j job.Job) {
		received2 <- stripTag(testID, j.JobID())
	}))
	defer p2.Close()

	consumer2, err := kfk.NewConsumer(brokers, groupID, topic, p2, tlsCfg)
	if err != nil {
		t.Fatalf("creating consumer 2: %v", err)
	}
	defer consumer2.Close()

	go func() {
		_ = consumer2.Start(ctx)
	}()

	waitForSync(t, ctx, producer, testID, sync2)

	// produce 2 more messages — consumer 2 is synced so these will be the
	// only test messages it sees (job-0,1,2 are past committed offsets).
	for i := 3; i < 5; i++ {
		j := job.MustNew(taggedJobID(testID, fmt.Sprintf("job-%d", i)), "https://example.com/webhook", now)
		if err := producer.Send(ctx, j); err != nil {
			t.Fatalf("sending message %d: %v", i, err)
		}
	}

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
	t.Parallel()
	testID := newTestID(t)
	groupID := newGroupID(t)

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
	var doneOnce sync.Once

	syncCh := make(chan struct{}, partitions)
	p := pool.New(2, syncingHandler(testID, syncCh, func(ctx context.Context, j job.Job) {
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
		doneOnce.Do(func() { close(done) })
	}))
	defer p.Close()

	consumer, err := kfk.NewConsumer(brokers, groupID, topic, p, tlsCfg, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		_ = consumer.Start(ctx)
	}()

	waitForSync(t, ctx, producer, testID, syncCh)

	tagged := taggedJobID(testID, "job-1")
	j := job.MustNew(tagged, "https://example.com/webhook", now)
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

	if attempts[tagged] != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts[tagged])
	}
	if stripTag(testID, retriedJob.JobID()) != "job-1" {
		t.Errorf("expected retried job to keep jobID job-1, got %s", stripTag(testID, retriedJob.JobID()))
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
