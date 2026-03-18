package kafka

import (
	"time"

	"github.com/matmazurk/oidc-keepie/job"
)

type kafkaJob struct {
	ID         string    `json:"id"`
	JobID      string    `json:"job_id"`
	WebhookURL string    `json:"webhook_url"`
	CreatedAt  time.Time `json:"created_at"`
}

func toKafkaJob(j job.Job) kafkaJob {
	return kafkaJob{
		ID:         j.ID(),
		JobID:      j.JobID(),
		WebhookURL: j.WebhookURL(),
		CreatedAt:  j.CreatedAt(),
	}
}

func (kj kafkaJob) toJob() (job.Job, error) {
	return job.New(kj.ID, kj.JobID, kj.WebhookURL, kj.CreatedAt)
}
