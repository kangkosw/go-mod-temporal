package execution

import "errors"

var (
	// ErrInvalidPolicy indicates invalid execution policy
	ErrInvalidPolicy = errors.New("invalid execution policy")

	// ErrExecutionTimeout indicates execution timeout
	ErrExecutionTimeout = errors.New("execution timeout")

	// ErrMaxRetriesExceeded indicates maximum retries exceeded
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")

	// ErrExecutionFailed indicates execution failed
	ErrExecutionFailed = errors.New("execution failed")

	// ErrInvalidTimeout indicates invalid timeout configuration
	ErrInvalidTimeout = errors.New("invalid timeout configuration")

	// ErrExecutionCanceled indicates execution was canceled
	ErrExecutionCanceled = errors.New("execution canceled")

	// ErrInvalidRetryPolicy indicates invalid retry policy
	ErrInvalidRetryPolicy = errors.New("invalid retry policy")

	// ErrExecutionNotFound indicates execution not found
	ErrExecutionNotFound = errors.New("execution not found")

	// ErrPolicyNotFound indicates policy not found
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrHandlerNotFound indicates handler not found
	ErrHandlerNotFound = errors.New("handler not found")
)
