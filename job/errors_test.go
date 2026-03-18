package job_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/matmazurk/oidc-keepie/job"
)

func TestIsRetryable(t *testing.T) {
	original := errors.New("connection refused")
	retryable := job.MakeRetryable(original)

	tests := []struct {
		name        string
		err         error
		isRetryable bool
	}{
		{"retryable error", retryable, true},
		{"wrapped retryable error", fmt.Errorf("handler failed: %w", retryable), true},
		{"regular error", errors.New("regular error"), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := job.IsRetryable(tt.err); got != tt.isRetryable {
				t.Errorf("IsRetryable() = %v, want %v", got, tt.isRetryable)
			}
		})
	}
}

func TestMakeRetryablePreservesOriginalError(t *testing.T) {
	original := errors.New("connection refused")
	retryable := job.MakeRetryable(original)

	if retryable.Error() != "connection refused" {
		t.Errorf("expected message 'connection refused', got '%s'", retryable.Error())
	}
	if !errors.Is(retryable, original) {
		t.Error("expected original error to be preserved via Unwrap")
	}
	if !errors.Is(fmt.Errorf("wrapped: %w", retryable), original) {
		t.Error("expected original error to be preserved through double wrapping")
	}
}
