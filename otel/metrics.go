package otel

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	instrumentsOnce sync.Once

	jobsProduced    metric.Int64Counter
	jobsConsumed    metric.Int64Counter
	jobsProcessed   metric.Int64Counter
	jobsFailed      metric.Int64Counter
	jobsRescheduled metric.Int64Counter
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

func topicAttr(topic string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("topic", topic))
}

func JobProduced(ctx context.Context, topic string) {
	initInstruments()
	jobsProduced.Add(ctx, 1, topicAttr(topic))
}

func JobConsumed(ctx context.Context, topic string) {
	initInstruments()
	jobsConsumed.Add(ctx, 1, topicAttr(topic))
}

func JobProcessed(ctx context.Context, topic string) {
	initInstruments()
	jobsProcessed.Add(ctx, 1, topicAttr(topic))
}

func JobFailed(ctx context.Context, topic string) {
	initInstruments()
	jobsFailed.Add(ctx, 1, topicAttr(topic))
}

func JobRescheduled(ctx context.Context, topic string) {
	initInstruments()
	jobsRescheduled.Add(ctx, 1, topicAttr(topic))
}

func UnmarshalError(ctx context.Context, topic string) {
	initInstruments()
	unmarshalErrors.Add(ctx, 1, topicAttr(topic))
}

func OffsetCommitError(ctx context.Context, topic string) {
	initInstruments()
	offsetCommitErrors.Add(ctx, 1, topicAttr(topic))
}

func RecordProcessingDuration(ctx context.Context, topic string, d time.Duration) {
	initInstruments()
	processingDuration.Record(ctx, d.Seconds(), topicAttr(topic))
}

func RecordTimeInQueue(ctx context.Context, topic string, d time.Duration) {
	initInstruments()
	timeInQueue.Record(ctx, d.Seconds(), topicAttr(topic))
}
