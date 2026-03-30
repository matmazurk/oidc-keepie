# pool

A fixed-size worker pool for concurrent job processing with pre-execution offset commits.

## Overview

The pool decouples job consumption from job processing. A caller (typically a Kafka consumer) submits jobs to the pool via a buffered channel. Each worker picks a job, commits its offset, and then runs the handler. This ensures at-least-once delivery semantics — the offset is committed before processing begins, so a crash during processing won't cause the message to be redelivered.

## Architecture

```mermaid
graph TD
    Consumer["Consumer (caller)"]
    Channel["Buffered Channel\n(capacity = size)"]
    W1["Worker 1"]
    W2["Worker 2"]
    WN["Worker N"]

    Consumer -->|"Submit(job)"| Channel
    Channel --> W1
    Channel --> W2
    Channel --> WN

    W1 --> Flow["1. Commit offset\n2. Run handler (on success)\nor skip handler (on error)"]
    W2 --> Flow
    WN --> Flow
```

## Worker flow

Each worker goroutine runs the same loop:

```mermaid
flowchart TD
    Wait["Wait for job\nfrom channel"] --> Commit["Call Commit()"]
    Commit --> Check{Success?}
    Check -->|yes| Handler["Run handler"]
    Check -->|no| Log["Log error\nSkip job"]
    Handler --> Wait
    Log --> Wait
```

## Submit and backpressure

`Submit` respects context cancellation. When the channel is full (all workers busy and buffer saturated), `Submit` blocks until either a slot opens or the context is cancelled:

```mermaid
flowchart TD
    Start["Submit(ctx, job)"] --> Closed{Pool closed?}
    Closed -->|yes| Err["return ErrPoolClosed"]
    Closed -->|no| Select["select"]
    Select -->|"channel ← job"| OK["return nil"]
    Select -->|"ctx.Done()"| CtxErr["return ctx.Err()"]
```

The buffered channel has capacity equal to the number of workers, so up to `2 * size` jobs can be outstanding at once (size in workers + size in buffer).

## Graceful shutdown

`Close` ensures all in-flight work finishes before returning:

```mermaid
flowchart TD
    Start["Close()"] --> Mark["Mark closed\n(rejects future Submits)"]
    Mark --> CloseCh["Close channel\n(workers drain remaining jobs)"]
    CloseCh --> WgWait["wg.Wait()\n(block until all workers exit)"]
```

## Usage

```go
p := pool.New(10, func(ctx context.Context, j job.Job) {
    // process job
})

err := p.Submit(ctx, pool.Job{
    Payload: j,
    Commit:  func() error { /* commit offset */ return nil },
})

// on shutdown
p.Close() // blocks until all workers finish
```
