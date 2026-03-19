package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matmazurk/oidc-keepie/handler"
	"github.com/matmazurk/oidc-keepie/job"
)

type mockIssuer struct {
	token []byte
	err   error
}

func (m *mockIssuer) Issue(_ context.Context) ([]byte, error) {
	return m.token, m.err
}

func TestHandler(t *testing.T) {
	tests := []struct {
		name       string
		issuer     *mockIssuer
		serverFunc func(w http.ResponseWriter, r *http.Request)
		wantErr    bool
		wantRetry  bool
	}{
		{
			name:   "success",
			issuer: &mockIssuer{token: []byte("test-token")},
			serverFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
		},
		{
			name:      "issuer error is retryable",
			issuer:    &mockIssuer{err: fmt.Errorf("signing failed")},
			wantErr:   true,
			wantRetry: true,
		},
		{
			name:   "webhook 4xx is not retryable",
			issuer: &mockIssuer{token: []byte("test-token")},
			serverFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
			},
			wantErr:   true,
			wantRetry: false,
		},
		{
			name:   "webhook 5xx is not retryable",
			issuer: &mockIssuer{token: []byte("test-token")},
			serverFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr:   true,
			wantRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var webhookURL string
			var client *http.Client

			if tt.serverFunc != nil {
				server := httptest.NewServer(http.HandlerFunc(tt.serverFunc))
				defer server.Close()
				webhookURL = server.URL
				client = http.DefaultClient
			} else {
				webhookURL = "http://example.com"
				client = http.DefaultClient
			}

			j := job.MustNew("job-1", webhookURL, time.Now())
			handle := handler.New(tt.issuer, client)

			err := handle(context.Background(), j)

			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantRetry && !job.IsRetryable(err) {
				t.Error("expected retryable error")
			}
			if tt.wantErr && !tt.wantRetry && job.IsRetryable(err) {
				t.Error("expected non-retryable error")
			}
		})
	}
}

func TestHandlerSendsTokenAsBody(t *testing.T) {
	token := []byte("my-jwt-token-data")
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf [1024]byte
		n, _ := r.Body.Read(buf[:])
		receivedBody = buf[:n]
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	j := job.MustNew("job-1", server.URL, time.Now())
	handle := handler.New(&mockIssuer{token: token}, http.DefaultClient)

	if err := handle(context.Background(), j); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(receivedBody) != string(token) {
		t.Errorf("expected body %q, got %q", token, receivedBody)
	}
}
