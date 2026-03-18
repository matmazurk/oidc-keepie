package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/matmazurk/oidc-keepie/job"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
)

type JobScheduler interface {
	Send(ctx context.Context, j job.Job) error
}

type Handler struct {
	scheduler JobScheduler
}

func NewHandler(scheduler JobScheduler) *Handler {
	return &Handler{scheduler: scheduler}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /jobs", h.scheduleJob)
}

type scheduleRequest struct {
	WebhookURL string `json:"webhook_url"`
}

type scheduleResponse struct {
	JobID string `json:"job_id"`
}

func (h *Handler) scheduleJob(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	keepieotel.HTTPRequest(ctx)

	var req scheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		keepieotel.HTTPInvalidBody(ctx)
		keepieotel.RecordHTTPDuration(ctx, time.Since(start))
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	jobID := uuid.NewString()
	j, err := job.New(jobID, req.WebhookURL, time.Now())
	if err != nil {
		keepieotel.HTTPInvalidBody(ctx)
		keepieotel.RecordHTTPDuration(ctx, time.Since(start))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.scheduler.Send(ctx, j); err != nil {
		slog.Error("scheduling job",
			slog.String("job_id", jobID),
			slog.String("error", err.Error()),
		)
		keepieotel.RecordHTTPDuration(ctx, time.Since(start))
		http.Error(w, "failed to schedule job", http.StatusInternalServerError)
		return
	}

	keepieotel.RecordHTTPDuration(ctx, time.Since(start))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(scheduleResponse{JobID: jobID})
}
