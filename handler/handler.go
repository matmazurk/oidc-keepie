package handler

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/matmazurk/oidc-keepie/job"
	keepieotel "github.com/matmazurk/oidc-keepie/otel"
)

type TokenIssuer interface {
	Issue(ctx context.Context) ([]byte, error)
}

func New(issuer TokenIssuer, client *http.Client) func(context.Context, job.Job) error {
	return func(ctx context.Context, j job.Job) error {
		ctx, span := keepieotel.Tracer().Start(ctx, "handler.process_job")
		defer span.End()

		token, err := issuer.Issue(ctx)
		if err != nil {
			span.RecordError(err)
			return job.MakeRetryable(fmt.Errorf("issuing token: %w", err))
		}

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

		if resp.StatusCode >= 400 {
			err := fmt.Errorf("webhook returned status %d", resp.StatusCode)
			span.RecordError(err)
			return err
		}

		return nil
	}
}
