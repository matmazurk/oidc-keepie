package handler

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/matmazurk/oidc-keepie/job"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
)

type Signer interface {
	Sign(ctx context.Context, payload []byte) ([]byte, error)
	Algorithm() string
	KeyID() string
}

type Config struct {
	Issuer   string
	Audience string
	TTL      time.Duration
}

func New(signer Signer, cfg Config, client *http.Client) func(context.Context, job.Job) error {
	header, _ := json.Marshal(jwtHeader{
		Algorithm: signer.Algorithm(),
		Type:      "JWT",
		KeyID:     signer.KeyID(),
	})
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)

	return func(ctx context.Context, j job.Job) error {
		ctx, span := keepieotel.Tracer().Start(ctx, "handler.process_job")
		defer span.End()

		now := time.Now()
		claims, err := json.Marshal(jwtClaims{
			Issuer:   cfg.Issuer,
			Subject:  j.JobID(),
			Audience: cfg.Audience,
			IssuedAt: now.Unix(),
			Expiry:   now.Add(cfg.TTL).Unix(),
			JWTID:    j.JobID(),
		})
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("encoding claims: %w", err)
		}

		signingInput := encodedHeader + "." + base64.RawURLEncoding.EncodeToString(claims)

		sig, err := signer.Sign(ctx, []byte(signingInput))
		if err != nil {
			span.RecordError(err)
			return job.MakeRetryable(fmt.Errorf("signing token: %w", err))
		}

		token := []byte(signingInput + "." + base64.RawURLEncoding.EncodeToString(sig))

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.WebhookURL(), bytes.NewReader(token))
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("creating request: %w", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("sending webhook: %w", err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)

		if resp.StatusCode >= 400 {
			err := fmt.Errorf("webhook returned status %d", resp.StatusCode)
			span.RecordError(err)
			return err
		}

		return nil
	}
}

type jwtHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

type jwtClaims struct {
	Issuer   string `json:"iss"`
	Subject  string `json:"sub"`
	Audience string `json:"aud"`
	IssuedAt int64  `json:"iat"`
	Expiry   int64  `json:"exp"`
	JWTID    string `json:"jti"`
}

type StubSigner struct{}

func (s *StubSigner) Sign(_ context.Context, _ []byte) ([]byte, error) {
	return []byte("stub-signature"), nil
}

func (s *StubSigner) Algorithm() string { return "HS256" }
func (s *StubSigner) KeyID() string     { return "stub-key" }
