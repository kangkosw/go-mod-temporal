package workflow

import "errors"

var (
	// ErrInvalidOptions indicates invalid workflow options
	ErrInvalidOptions = errors.New("invalid workflow options")

	// ErrInvalidWorkflowID indicates invalid workflow ID
	ErrInvalidWorkflowID = errors.New("invalid workflow ID")

	// ErrInvalidTaskQueue indicates invalid task queue
	ErrInvalidTaskQueue = errors.New("invalid task queue")

	// ErrWorkflowNotFound indicates workflow not found
	ErrWorkflowNotFound = errors.New("workflow not found")

	// ErrWorkflowAlreadyCompleted indicates workflow already completed
	ErrWorkflowAlreadyCompleted = errors.New("workflow already completed")

	// ErrInvalidSignal indicates invalid signal
	ErrInvalidSignal = errors.New("invalid signal")

	// ErrInvalidQuery indicates invalid query
	ErrInvalidQuery = errors.New("invalid query")
)
