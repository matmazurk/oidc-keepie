package otel

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

var (
	instrumentsOnce sync.Once

	jobsProduced       metric.Int64Counter
	jobsConsumed       metric.Int64Counter
	jobsProcessed      metric.Int64Counter
	jobsFailed         metric.Int64Counter
	jobsRescheduled    metric.Int64Counter
	unmarshalErrors    metric.Int64Counter
	offsetCommitErrors metric.Int64Counter

	processingDuration metric.Float64Histogram
	timeInQueue        metric.Float64Histogram
)

func initInstruments() {
	instrumentsOnce.Do(func() {
		m := Meter()

		jobsProduced, _ = m.Int64Counter("keepie.jobs.produced",
			metric.WithDescription("Number of jobs sent to Kafka"),
		)
		jobsConsumed, _ = m.Int64Counter("keepie.jobs.consumed",
			metric.WithDescription("Number of jobs received from Kafka"),
		)
		jobsProcessed, _ = m.Int64Counter("keepie.jobs.processed",
			metric.WithDescription("Number of jobs processed successfully"),
		)
		jobsFailed, _ = m.Int64Counter("keepie.jobs.failed",
			metric.WithDescription("Number of jobs that failed with non-retryable error"),
		)
		jobsRescheduled, _ = m.Int64Counter("keepie.jobs.rescheduled",
			metric.WithDescription("Number of jobs rescheduled after retryable error"),
		)
		unmarshalErrors, _ = m.Int64Counter("keepie.jobs.unmarshal_errors",
			metric.WithDescription("Number of messages that failed to unmarshal"),
		)
		offsetCommitErrors, _ = m.Int64Counter("keepie.consumer.offset_commit_errors",
			metric.WithDescription("Number of offset commits that failed"),
		)

		processingDuration, _ = m.Float64Histogram("keepie.jobs.processing_duration_seconds",
			metric.WithDescription("Time spent processing a job in the handler"),
			metric.WithUnit("s"),
		)
		timeInQueue, _ = m.Float64Histogram("keepie.jobs.time_in_queue_seconds",
			metric.WithDescription("Time between job creation or rescheduling and consumption"),
			metric.WithUnit("s"),
		)
	})
}

func JobProduced(ctx context.Context) {
	initInstruments()
	jobsProduced.Add(ctx, 1)
}

func JobConsumed(ctx context.Context) {
	initInstruments()
	jobsConsumed.Add(ctx, 1)
}

func JobProcessed(ctx context.Context) {
	initInstruments()
	jobsProcessed.Add(ctx, 1)
}

func JobFailed(ctx context.Context) {
	initInstruments()
	jobsFailed.Add(ctx, 1)
}

func JobRescheduled(ctx context.Context) {
	initInstruments()
	jobsRescheduled.Add(ctx, 1)
}

func UnmarshalError(ctx context.Context) {
	initInstruments()
	unmarshalErrors.Add(ctx, 1)
}

func OffsetCommitError(ctx context.Context) {
	initInstruments()
	offsetCommitErrors.Add(ctx, 1)
}

func RecordProcessingDuration(ctx context.Context, d time.Duration) {
	initInstruments()
	processingDuration.Record(ctx, d.Seconds())
}

func RecordTimeInQueue(ctx context.Context, d time.Duration) {
	initInstruments()
	timeInQueue.Record(ctx, d.Seconds())
}
