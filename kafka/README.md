# Kafka Client

Uses [franz-go](https://github.com/twmb/franz-go) (`kgo`) for both producing and consuming. Chosen over sarama for synchronous offset commit with error handling.

## Configuration

| Setting | Value | Why |
|---|---|---|
| Library | `twmb/franz-go` | Synchronous `CommitRecords` returns error, unlike sarama's fire-and-forget |
| Partitions | 2 | Allows scaling up to 2 consumer instances |
| Offset commit | Manual, commit-first | Commit before processing to prevent duplicate JWTs |
| Partitioning | Round-robin (no key) | Jobs are independent, we want even distribution |
| Auto-offset-reset | `earliest` | On first start or expired offsets, process from beginning rather than skip |
| Transport | mTLS (required) | Broker identity + client auth via X.509; plaintext is not supported |

## mTLS

The client connects over mTLS only. At startup, `NewProducer` and
`NewConsumer` require a non-nil `*tls.Config`. The `cmd/keepie` binary builds
one via `kafka.LoadTLSConfig(...)` from these env vars (all required):

| Env var | Description |
|---|---|
| `KAFKA_TLS_CA_FILE` | PEM file with the CA that signed the broker cert |
| `KAFKA_TLS_CERT_FILE` | Client certificate PEM |
| `KAFKA_TLS_KEY_FILE` | Client private key PEM |

## Integration tests

Integration tests (`//go:build integration`) connect to an **existing**
kafka broker — they no longer spin up a testcontainer. Set these env vars
before running them:

| Env var | Description |
|---|---|
| `KAFKA_TEST_BROKERS` | Comma-separated broker list. If unset, all integration tests skip. |
| `KAFKA_TEST_CA_FILE` | CA PEM (required when `KAFKA_TEST_BROKERS` is set) |
| `KAFKA_TEST_CERT_FILE` | Client cert PEM (required when `KAFKA_TEST_BROKERS` is set) |
| `KAFKA_TEST_KEY_FILE` | Client key PEM (required when `KAFKA_TEST_BROKERS` is set) |

Run with: `go test -tags=integration ./kafka/...`

---

## How the client works

### Producer

The producer sends job messages to the Kafka topic. The topic is configured at creation time. Messages are JSON-encoded and distributed across partitions using round-robin (no message key).

```mermaid
sequenceDiagram
    participant App
    participant Producer
    participant Kafka

    App->>Producer: Send(ctx, job)
    Producer->>Producer: Convert job.Job → kafkaJob (add event ID + JSON tags)
    Producer->>Producer: Marshal to JSON
    Producer->>Kafka: ProduceSync (round-robin partition)
    Kafka-->>Producer: Acknowledgment
    Producer-->>App: nil (success)
```

### Consumer

The consumer joins a consumer group. Kafka assigns partitions to it. `PollFetches` returns batches of records from all assigned partitions. Records are processed sequentially, one at a time.

```mermaid
sequenceDiagram
    participant Kafka
    participant Consumer
    participant Handler

    loop Poll loop
        Consumer->>Kafka: Poll for records
        Kafka-->>Consumer: Batch of records

        loop Each record (sequential)
            Consumer->>Kafka: Commit offset
            Consumer->>Consumer: Unmarshal record
            Consumer->>Handler: Process job
        end
    end
```

### Scaling

Throughput scales by adding consumer instances (each gets a subset of partitions). Within a single instance, processing is sequential.

1. **Instance 1 starts** — gets both partitions
2. **Instance 2 starts** — triggers rebalance, each instance gets 1 partition
3. **Instance 2 dies** — triggers rebalance, instance 1 gets both back

Maximum useful consumer instances = number of partitions. Extra instances sit idle as hot standbys.

---

## Commit strategy and failure points

We commit the offset **before** processing the job. This is a deliberate design choice driven by the fact that our job (generating a JWT and sending it to a webhook) is **not idempotent** — sending two different JWTs for the same request is worse than losing a single job.

With franz-go, `CommitRecords` is synchronous and returns an error. If the commit fails, we skip the record entirely — it will be redelivered on the next poll.

### Message processing flow

```mermaid
graph TD
    A[Receive record] --> B[CommitRecords]
    B -->|error| B1[Log + metric, skip]
    B -->|success| C[Unmarshal JSON]
    C -->|error| C1[Log + metric, skip]
    C -->|success| D[Call handler]
    D --> E{Handler result?}
    E -->|success| F[Done]
    E -->|retryable error| G[Re-send to topic]
    E -->|non-retryable error| H[Log error]

    style B fill:#ff9999,stroke:#cc0000
    style D fill:#ff9999,stroke:#cc0000
```

### Where jobs can be lost

> **The red-highlighted steps above are the danger zones.** A crash at these points means a committed offset with no processing.

| Failure point | What happens | Job lost? |
|---|---|---|
| Commit fails | Record skipped, will be redelivered on next poll. | No |
| **After commit, before handler starts** | **Offset committed, handler never ran.** | **YES** |
| **During handler execution (before webhook)** | **Offset committed, JWT never sent.** | **YES** |
| After webhook sent, before response received | Ambiguous — webhook may or may not have received the JWT. This is unavoidable in any distributed system. | Maybe |
| After successful handler return | Everything succeeded. | No |

**Why this is acceptable:** A lost job can be recovered — the client can retry their request. But a duplicate JWT (two different tokens for the same request) creates real confusion that cannot be automatically resolved.

### Retryable errors

When a handler returns an error wrapped with `job.MakeRetryable()`, the consumer creates a rescheduled copy of the job and sends it back to the topic. The rescheduled job has:

- A **new event ID** (UUID, generated by the Kafka layer) — so each attempt is uniquely identifiable in Kafka
- The **same job ID** — ties all attempts to the original logical job
- The **same createdAt** — preserves the original creation time
- A **rescheduledAt** timestamp — when this retry was scheduled
- An incremented **retryCount**

Note: the event ID is a Kafka transport concern, not part of the domain `job.Job` type. It is generated in the `kafkaJob` mapping layer when a job is sent to the topic.

```mermaid
sequenceDiagram
    participant Kafka
    participant Consumer
    participant Handler

    Kafka->>Consumer: Message (job-1, retryCount=0)
    Consumer->>Kafka: CommitRecords (success)
    Consumer->>Consumer: Unmarshal (success)
    Consumer->>Handler: handler(ctx, job)
    Handler-->>Consumer: RetryableError("webhook timeout")
    Consumer->>Consumer: job.Reschedule(now)
    Consumer->>Kafka: Re-send (job-1, new-evt-id, retryCount=1, rescheduledAt=now)
    Note over Kafka: Job re-enters the topic<br/>with new event ID

    Kafka->>Consumer: Message (job-1, retryCount=1)
    Consumer->>Kafka: CommitRecords (success)
    Consumer->>Consumer: Unmarshal (success)
    Consumer->>Handler: handler(ctx, job)
    Handler-->>Consumer: nil (success)
```

---

## Graceful shutdown

When the service receives a shutdown signal (SIGINT/SIGTERM):

```mermaid
sequenceDiagram
    participant Signal
    participant App
    participant Consumer
    participant Kafka

    Signal->>App: SIGTERM
    App->>App: Cancel context
    Note over Consumer: PollFetches returns (ctx cancelled)
    Consumer->>Consumer: Start() returns
    App->>Consumer: Close()
    Consumer->>Kafka: LeaveGroup
    Note over Kafka: Remaining consumers<br/>get rebalanced immediately
```

The current record finishes processing before `PollFetches` is called again and the context cancellation is detected.

---

## Metrics

All metrics are defined in the `otel` package and recorded via function calls (no metric instruments leak into other packages).

### Kafka metrics

| Metric | Type | Description |
|---|---|---|
| `keepie.jobs.scheduled` | Counter | Jobs sent to Kafka |
| `keepie.jobs.consumed` | Counter | Jobs received from Kafka |
| `keepie.jobs.processed` | Counter | Jobs processed successfully |
| `keepie.jobs.failed` | Counter | Jobs failed with non-retryable error |
| `keepie.jobs.rescheduled` | Counter | Jobs rescheduled after retryable error |
| `keepie.jobs.unmarshal_errors` | Counter | Messages that failed to unmarshal |
| `keepie.consumer.offset_commit_errors` | Counter | Offset commits that failed |
| `keepie.jobs.processing_duration_seconds` | Histogram | Time spent in the handler |
| `keepie.jobs.time_in_queue_seconds` | Histogram | Time between creation/rescheduling and consumption |

---

## Integration tests

All tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up a real Kafka instance in Docker. No manual setup needed.

Run with:
```bash
go test -tags=integration -v -timeout 300s ./kafka/...
```

### Test 1: Produce and Consume

**What it tests:** Basic end-to-end message flow — a produced message arrives at the consumer with all fields intact.

```mermaid
sequenceDiagram
    participant Test
    participant Producer
    participant Kafka
    participant Consumer

    Test->>Producer: Send job (job-1, webhook-url)
    Producer->>Kafka: Message
    Kafka->>Consumer: Deliver message
    Consumer->>Test: Received job
    Test->>Test: Assert: JobID, WebhookURL, CreatedAt match
```

### Test 2: Workload Distribution

**What it tests:** 200 messages are distributed across 2 consumers in the same group. Each message is processed by exactly one consumer — no duplicates, no missed messages.

```mermaid
graph LR
    P[Producer<br/>200 messages] --> K[Kafka<br/>2 partitions]
    K --> C1[Consumer 1<br/>~100 messages]
    K --> C2[Consumer 2<br/>~100 messages]

    subgraph "Same consumer group"
        C1
        C2
    end
```

**Assertions:**
- Total messages received = 200
- Both consumers received at least 1 message
- No duplicate job IDs across consumers

### Test 3: Consumer Rebalancing

**What it tests:** When a consumer leaves the group, its partitions are reassigned to the remaining consumer.

```mermaid
sequenceDiagram
    participant Test
    participant C1 as Consumer 1
    participant C2 as Consumer 2
    participant Kafka

    Note over C1,C2: Phase 1: Both consumers active
    Test->>Kafka: Send 5 messages
    Kafka->>C1: Some messages
    Kafka->>C2: Some messages

    Test->>C1: Stop (cancel context + close)
    Note over Kafka: Rebalance: all partitions → Consumer 2

    Note over C2: Phase 2: Only Consumer 2 active
    Test->>Kafka: Send 5 more messages
    Kafka->>C2: All 5 messages
    Test->>Test: Assert: Consumer 2 received all phase 2 messages
```

### Test 4: Offset Persistence

**What it tests:** After a consumer commits offsets and restarts, a new consumer in the same group resumes from where the previous one left off — it does not re-receive already committed messages.

```mermaid
sequenceDiagram
    participant Test
    participant C1 as Consumer 1
    participant C2 as Consumer 2
    participant Kafka

    Test->>Kafka: Send 3 messages (job-0, job-1, job-2)
    Kafka->>C1: Deliver all 3
    C1->>Kafka: Commit offsets
    Test->>C1: Stop

    Test->>Kafka: Send 2 more (job-3, job-4)
    Test->>C2: Start (same group ID)
    Kafka->>C2: Deliver only job-3, job-4
    Test->>Test: Assert: exactly 2 messages, no redelivery
```

### Test 5: Retryable Error

**What it tests:** When a handler returns a retryable error, the job is rescheduled with incremented retry count. The second attempt succeeds.

```mermaid
sequenceDiagram
    participant Test
    participant Consumer
    participant Kafka

    Test->>Kafka: Send job (job-1, retryCount=0)
    Kafka->>Consumer: Deliver job-1 (attempt 1)
    Consumer->>Consumer: Handler returns RetryableError
    Consumer->>Kafka: Reschedule (job-1, retryCount=1, rescheduledAt=now)
    Kafka->>Consumer: Deliver job-1 (attempt 2)
    Consumer->>Consumer: Handler returns nil (success)
    Test->>Test: Assert: 2 attempts, retryCount=1,<br/>rescheduledAt set, createdAt preserved
```
