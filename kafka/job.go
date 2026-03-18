package kafka

import (
	"time"

	"github.com/google/uuid"
	"github.com/matmazurk/oidc-keepie/job"
)

type kafkaJob struct {
	EventID       string    `json:"event_id"`
	JobID         string    `json:"job_id"`
	WebhookURL    string    `json:"webhook_url"`
	CreatedAt     time.Time `json:"created_at"`
	RescheduledAt time.Time `json:"rescheduled_at,omitempty"`
	RetryCount    int       `json:"retry_count,omitempty"`
}

func toKafkaJob(j job.Job) kafkaJob {
	return kafkaJob{
		EventID:       uuid.NewString(),
		JobID:         j.JobID(),
		WebhookURL:    j.WebhookURL(),
		CreatedAt:     j.CreatedAt(),
		RescheduledAt: j.RescheduledAt(),
		RetryCount:    j.RetryCount(),
	}
}

func (kj kafkaJob) toJob() (job.Job, error) {
	return job.Restore(kj.JobID, kj.WebhookURL, kj.CreatedAt, kj.RescheduledAt, kj.RetryCount)
}
