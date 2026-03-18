package job

import (
	"errors"
	"net/url"
	"time"
)

type Job struct {
	id         string
	jobID      string
	webhookURL string
	createdAt  time.Time
}

func New(id, jobID, webhookURL string, createdAt time.Time) (Job, error) {
	if id == "" {
		return Job{}, errors.New("id must not be empty")
	}
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
		id:         id,
		jobID:      jobID,
		webhookURL: webhookURL,
		createdAt:  createdAt,
	}, nil
}

func (j Job) ID() string {
	return j.id
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
