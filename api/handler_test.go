package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/matmazurk/oidc-keepie/api"
	"github.com/matmazurk/oidc-keepie/job"
)

type mockScheduler struct {
	lastJob job.Job
	err     error
}

func (m *mockScheduler) Send(_ context.Context, j job.Job) error {
	m.lastJob = j
	return m.err
}

func TestScheduleJob(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		schedulerErr   error
		wantStatus     int
		wantJobID      bool
		wantWebhookURL string
	}{
		{
			name:           "valid request",
			body:           `{"webhook_url": "https://example.com/callback"}`,
			wantStatus:     http.StatusAccepted,
			wantJobID:      true,
			wantWebhookURL: "https://example.com/callback",
		},
		{
			name:       "invalid json",
			body:       `not json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty webhook url",
			body:       `{"webhook_url": ""}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid webhook url",
			body:       `{"webhook_url": "not-a-url"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-https webhook url",
			body:       `{"webhook_url": "http://example.com/callback"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:         "scheduler error",
			body:         `{"webhook_url": "https://example.com/callback"}`,
			schedulerErr: fmt.Errorf("kafka down"),
			wantStatus:   http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheduler := &mockScheduler{err: tt.schedulerErr}
			handler := api.NewHandler(scheduler)

			mux := http.NewServeMux()
			handler.RegisterRoutes(mux)

			req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			mux.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, rec.Code)
			}

			if tt.wantJobID {
				var resp struct {
					JobID string `json:"job_id"`
				}
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatalf("decoding response: %v", err)
				}
				if resp.JobID == "" {
					t.Error("expected non-empty job_id in response")
				}
				if scheduler.lastJob.WebhookURL() != tt.wantWebhookURL {
					t.Errorf("expected webhook URL %s, got %s", tt.wantWebhookURL, scheduler.lastJob.WebhookURL())
				}
			}
		})
	}
}
