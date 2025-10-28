package client

import "errors"

var (
	// ErrInvalidHostPort indicates invalid host:port configuration
	ErrInvalidHostPort = errors.New("invalid host:port configuration")

	// ErrInvalidNamespace indicates invalid namespace configuration
	ErrInvalidNamespace = errors.New("invalid namespace configuration")

	// ErrClientNotConnected indicates client is not connected
	ErrClientNotConnected = errors.New("client is not connected")

	// ErrInvalidWorkflowID indicates invalid workflow ID
	ErrInvalidWorkflowID = errors.New("invalid workflow ID")

	// ErrInvalidTaskQueue indicates invalid task queue
	ErrInvalidTaskQueue = errors.New("invalid task queue")

	// ErrInvalidConfiguration indicates invalid configuration
	ErrInvalidConfiguration = errors.New("invalid configuration")
)
