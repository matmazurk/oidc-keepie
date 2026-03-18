package job

import (
	"errors"
	"net/url"
	"time"
)

type Job struct {
	jobID         string
	webhookURL    string
	createdAt     time.Time
	rescheduledAt time.Time
	retryCount    int
}

func New(jobID, webhookURL string, createdAt time.Time) (Job, error) {
	if jobID == "" {
		return Job{}, errors.New("jobID must not be empty")
	}
	if webhookURL == "" {
		return Job{}, errors.New("webhookURL must not be empty")
	}
	if _, err := url.ParseRequestURI(webhookURL); err != nil {
		return Job{}, errors.New("webhookURL must be a valid URL")
	}
	if createdAt.IsZero() {
		return Job{}, errors.New("createdAt must not be zero")
	}
	return Job{
		jobID:      jobID,
		webhookURL: webhookURL,
		createdAt:  createdAt,
	}, nil
}

func Restore(jobID, webhookURL string, createdAt, rescheduledAt time.Time, retryCount int) (Job, error) {
	j, err := New(jobID, webhookURL, createdAt)
	if err != nil {
		return Job{}, err
	}
	j.rescheduledAt = rescheduledAt
	j.retryCount = retryCount
	return j, nil
}

func MustNew(jobID, webhookURL string, createdAt time.Time) Job {
	j, err := New(jobID, webhookURL, createdAt)
	if err != nil {
		panic(err)
	}
	return j
}

func (j Job) Reschedule(rescheduledAt time.Time) Job {
	return Job{
		jobID:         j.jobID,
		webhookURL:    j.webhookURL,
		createdAt:     j.createdAt,
		rescheduledAt: rescheduledAt,
		retryCount:    j.retryCount + 1,
	}
}

func (j Job) JobID() string {
	return j.jobID
}

func (j Job) WebhookURL() string {
	return j.webhookURL
}

func (j Job) CreatedAt() time.Time {
	return j.createdAt
}

func (j Job) RescheduledAt() time.Time {
	return j.rescheduledAt
}

func (j Job) RetryCount() int {
	return j.retryCount
}
