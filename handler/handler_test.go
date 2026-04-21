package handler_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/matmazurk/oidc-keepie/handler"
	"github.com/matmazurk/oidc-keepie/job"
)

type mockSigner struct {
	sig []byte
	err error
}

func (m *mockSigner) Sign(_ context.Context, _ []byte) ([]byte, error) {
	return m.sig, m.err
}

func (m *mockSigner) Algorithm() string { return "HS256" }
func (m *mockSigner) KeyID() string     { return "test-key" }

var testConfig = handler.Config{
	Issuer:   "https://keepie.example.com",
	Audience: "https://api.example.com",
	TTL:      5 * time.Minute,
}

func TestHandler(t *testing.T) {
	tests := []struct {
		name       string
		signer     *mockSigner
		serverFunc func(w http.ResponseWriter, r *http.Request)
		wantErr    bool
		wantRetry  bool
	}{
		{
			name:   "success",
			signer: &mockSigner{sig: []byte("test-signature")},
			serverFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
		},
		{
			name:      "signer error is retryable",
			signer:    &mockSigner{err: fmt.Errorf("HSM unavailable")},
			wantErr:   true,
			wantRetry: true,
		},
		{
			name:   "webhook 4xx is not retryable",
			signer: &mockSigner{sig: []byte("test-signature")},
			serverFunc: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
			},
			wantErr:   true,
			wantRetry: false,
		},
		{
			name:   "webhook 5xx is not retryable",
			signer: &mockSigner{sig: []byte("test-signature")},
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
			handle := handler.New(tt.signer, testConfig, client)

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

func TestHandlerSendsValidJWT(t *testing.T) {
	var receivedBody string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(strings.Builder)
		_, _ = io.Copy(buf, r.Body)
		receivedBody = buf.String()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	signer := &mockSigner{sig: []byte("test-signature")}
	j := job.MustNew("job-1", server.URL, time.Now())
	handle := handler.New(signer, testConfig, http.DefaultClient)

	if err := handle(context.Background(), j); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parts := strings.Split(receivedBody, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT parts, got %d", len(parts))
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decoding header: %v", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		t.Fatalf("unmarshalling header: %v", err)
	}
	if header.Alg != "HS256" {
		t.Errorf("expected alg HS256, got %s", header.Alg)
	}
	if header.Typ != "JWT" {
		t.Errorf("expected typ JWT, got %s", header.Typ)
	}
	if header.Kid != "test-key" {
		t.Errorf("expected kid test-key, got %s", header.Kid)
	}

	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decoding claims: %v", err)
	}
	var claims struct {
		Iss string `json:"iss"`
		Sub string `json:"sub"`
		Aud string `json:"aud"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
		Jti string `json:"jti"`
	}
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		t.Fatalf("unmarshalling claims: %v", err)
	}
	if claims.Iss != testConfig.Issuer {
		t.Errorf("expected iss %s, got %s", testConfig.Issuer, claims.Iss)
	}
	if claims.Sub != "job-1" {
		t.Errorf("expected sub job-1, got %s", claims.Sub)
	}
	if claims.Aud != testConfig.Audience {
		t.Errorf("expected aud %s, got %s", testConfig.Audience, claims.Aud)
	}
	if claims.Jti != "job-1" {
		t.Errorf("expected jti job-1, got %s", claims.Jti)
	}
	if claims.Exp-claims.Iat != int64(testConfig.TTL.Seconds()) {
		t.Errorf("expected TTL %v, got %d seconds", testConfig.TTL, claims.Exp-claims.Iat)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decoding signature: %v", err)
	}
	if string(sig) != "test-signature" {
		t.Errorf("expected signature 'test-signature', got %q", sig)
	}
}
