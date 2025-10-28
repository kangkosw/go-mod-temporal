package activity

import "errors"

var (
	// ErrInvalidActivity indicates invalid activity function
	ErrInvalidActivity = errors.New("invalid activity function")

	// ErrActivityNotFound indicates activity not found in registry
	ErrActivityNotFound = errors.New("activity not found")

	// ErrInvalidActivityOptions indicates invalid activity options
	ErrInvalidActivityOptions = errors.New("invalid activity options")

	// ErrActivityTimeout indicates activity timeout
	ErrActivityTimeout = errors.New("activity timeout")

	// ErrActivityCanceled indicates activity was canceled
	ErrActivityCanceled = errors.New("activity canceled")

	// ErrInvalidHeartbeat indicates invalid heartbeat data
	ErrInvalidHeartbeat = errors.New("invalid heartbeat data")

	// ErrHeartbeatTimeout indicates heartbeat timeout
	ErrHeartbeatTimeout = errors.New("heartbeat timeout")

	// ErrInvalidRetryPolicy indicates invalid retry policy
	ErrInvalidRetryPolicy = errors.New("invalid retry policy")

	// ErrMaxRetriesExceeded indicates maximum retries exceeded
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")

	// ErrInvalidLocalActivity indicates invalid local activity
	ErrInvalidLocalActivity = errors.New("invalid local activity")

	// ErrLocalActivityNotFound indicates local activity not found
	ErrLocalActivityNotFound = errors.New("local activity not found")
)
