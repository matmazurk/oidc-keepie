package job

import "errors"

func MakeRetryable(err error) error {
	return &retryableError{err: err}
}

func IsRetryable(err error) bool {
	var retryErr retryable
	return errors.As(err, &retryErr)
}

type retryable interface {
	isRetryable()
}

type retryableError struct {
	err error
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

func (e *retryableError) Unwrap() error {
	return e.err
}

func (e *retryableError) isRetryable() {}
