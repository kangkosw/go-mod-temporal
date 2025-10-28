package utils

import (
	"context"
	"time"

	"go.temporal.io/sdk/temporal"
)

// RetryPolicyBuilder helps build retry policies
type RetryPolicyBuilder struct {
	policy *temporal.RetryPolicy
}

// NewRetryPolicyBuilder creates a new retry policy builder
func NewRetryPolicyBuilder() *RetryPolicyBuilder {
	return &RetryPolicyBuilder{
		policy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
}

// InitialInterval sets the initial retry interval
func (b *RetryPolicyBuilder) InitialInterval(interval time.Duration) *RetryPolicyBuilder {
	b.policy.InitialInterval = interval
	return b
}

// BackoffCoefficient sets the backoff coefficient
func (b *RetryPolicyBuilder) BackoffCoefficient(coefficient float64) *RetryPolicyBuilder {
	b.policy.BackoffCoefficient = coefficient
	return b
}

// MaximumInterval sets the maximum retry interval
func (b *RetryPolicyBuilder) MaximumInterval(interval time.Duration) *RetryPolicyBuilder {
	b.policy.MaximumInterval = interval
	return b
}

// MaximumAttempts sets the maximum number of retry attempts
func (b *RetryPolicyBuilder) MaximumAttempts(attempts int32) *RetryPolicyBuilder {
	b.policy.MaximumAttempts = attempts
	return b
}

// NonRetryableErrorTypes sets the non-retryable error types
func (b *RetryPolicyBuilder) NonRetryableErrorTypes(errorTypes []string) *RetryPolicyBuilder {
	b.policy.NonRetryableErrorTypes = errorTypes
	return b
}

// Build builds the retry policy
func (b *RetryPolicyBuilder) Build() *temporal.RetryPolicy {
	return b.policy
}

// Predefined retry policies

// QuickRetryPolicy returns a retry policy for quick operations
func QuickRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(100 * time.Millisecond).
		BackoffCoefficient(1.5).
		MaximumInterval(5 * time.Second).
		MaximumAttempts(5).
		Build()
}

// StandardRetryPolicy returns a standard retry policy
func StandardRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(1 * time.Second).
		BackoffCoefficient(2.0).
		MaximumInterval(30 * time.Second).
		MaximumAttempts(3).
		Build()
}

// PatientRetryPolicy returns a retry policy for external services
func PatientRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(10 * time.Second).
		BackoffCoefficient(2.0).
		MaximumInterval(5 * time.Minute).
		MaximumAttempts(5).
		Build()
}

// AggressiveRetryPolicy returns an aggressive retry policy
func AggressiveRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(50 * time.Millisecond).
		BackoffCoefficient(1.2).
		MaximumInterval(2 * time.Second).
		MaximumAttempts(10).
		Build()
}

// NoRetryPolicy returns a policy with no retries
func NoRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		MaximumAttempts(1).
		Build()
}

// DatabaseRetryPolicy returns a retry policy for database operations
func DatabaseRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(500 * time.Millisecond).
		BackoffCoefficient(2.0).
		MaximumInterval(10 * time.Second).
		MaximumAttempts(5).
		NonRetryableErrorTypes([]string{
			"InvalidArgument",
			"PermissionDenied",
			"NotFound",
		}).
		Build()
}

// HTTPRetryPolicy returns a retry policy for HTTP operations
func HTTPRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(1 * time.Second).
		BackoffCoefficient(2.0).
		MaximumInterval(30 * time.Second).
		MaximumAttempts(4).
		NonRetryableErrorTypes([]string{
			"400", "401", "403", "404", "422", // Client errors
		}).
		Build()
}

// FileIORetryPolicy returns a retry policy for file I/O operations
func FileIORetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(200 * time.Millisecond).
		BackoffCoefficient(1.5).
		MaximumInterval(5 * time.Second).
		MaximumAttempts(3).
		NonRetryableErrorTypes([]string{
			"PermissionDenied",
			"NotFound",
			"InvalidPath",
		}).
		Build()
}

// NetworkRetryPolicy returns a retry policy for network operations
func NetworkRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(2 * time.Second).
		BackoffCoefficient(2.0).
		MaximumInterval(1 * time.Minute).
		MaximumAttempts(5).
		Build()
}

// CriticalRetryPolicy returns a retry policy for critical operations
func CriticalRetryPolicy() *temporal.RetryPolicy {
	return NewRetryPolicyBuilder().
		InitialInterval(100 * time.Millisecond).
		BackoffCoefficient(1.1).
		MaximumInterval(1 * time.Second).
		MaximumAttempts(20).
		Build()
}

// Utility functions for retry calculations

// CalculateRetryDelay calculates the delay for a specific retry attempt
func CalculateRetryDelay(policy *temporal.RetryPolicy, attempt int32) time.Duration {
	if policy == nil || attempt <= 1 {
		return 0
	}

	delay := policy.InitialInterval
	for i := int32(2); i <= attempt; i++ {
		delay = time.Duration(float64(delay) * policy.BackoffCoefficient)
		if delay > policy.MaximumInterval {
			delay = policy.MaximumInterval
			break
		}
	}

	return delay
}

// CalculateTotalRetryTime calculates the total time for all retries
func CalculateTotalRetryTime(policy *temporal.RetryPolicy) time.Duration {
	if policy == nil || policy.MaximumAttempts <= 1 {
		return 0
	}

	totalTime := time.Duration(0)
	delay := policy.InitialInterval

	for i := int32(2); i <= policy.MaximumAttempts; i++ {
		totalTime += delay
		delay = time.Duration(float64(delay) * policy.BackoffCoefficient)
		if delay > policy.MaximumInterval {
			delay = policy.MaximumInterval
		}
	}

	return totalTime
}

// IsRetryableError checks if an error is retryable based on policy
func IsRetryableError(policy *temporal.RetryPolicy, err error) bool {
	if policy == nil || err == nil {
		return true
	}

	errorType := err.Error()
	for _, nonRetryable := range policy.NonRetryableErrorTypes {
		if errorType == nonRetryable {
			return false
		}
	}

	return true
}

// RetryStats holds retry statistics
type RetryStats struct {
	TotalAttempts   int32
	SuccessfulRetry bool
	TotalTime       time.Duration
	LastError       error
}

// RetryExecutor executes functions with retry logic
type RetryExecutor struct {
	policy *temporal.RetryPolicy
	logger Logger
}

// NewRetryExecutor creates a new retry executor
func NewRetryExecutor(policy *temporal.RetryPolicy, logger Logger) *RetryExecutor {
	if policy == nil {
		policy = StandardRetryPolicy()
	}
	return &RetryExecutor{
		policy: policy,
		logger: logger,
	}
}

// Execute executes a function with retry logic
func (r *RetryExecutor) Execute(fn func() error) (*RetryStats, error) {
	stats := &RetryStats{}
	startTime := time.Now()

	for attempt := int32(1); attempt <= r.policy.MaximumAttempts; attempt++ {
		stats.TotalAttempts = attempt

		// Execute function
		err := fn()
		if err == nil {
			stats.SuccessfulRetry = true
			stats.TotalTime = time.Since(startTime)
			return stats, nil
		}

		stats.LastError = err

		// Check if error is retryable
		if !IsRetryableError(r.policy, err) {
			r.logger.Error("Non-retryable error encountered",
				"error", err,
				"attempt", attempt,
			)
			break
		}

		// Check if we have more attempts
		if attempt >= r.policy.MaximumAttempts {
			r.logger.Error("Max retry attempts exceeded",
				"error", err,
				"attempts", attempt,
			)
			break
		}

		// Calculate and wait for retry delay
		delay := CalculateRetryDelay(r.policy, attempt+1)
		r.logger.Warn("Retrying after error",
			"error", err,
			"attempt", attempt,
			"nextAttempt", attempt+1,
			"delay", delay,
		)

		time.Sleep(delay)
	}

	stats.TotalTime = time.Since(startTime)
	return stats, stats.LastError
}

// Context-aware retry executor

// ExecuteWithContext executes a function with context and retry logic
func (r *RetryExecutor) ExecuteWithContext(ctx context.Context, fn func(context.Context) error) (*RetryStats, error) {
	stats := &RetryStats{}
	startTime := time.Now()

	for attempt := int32(1); attempt <= r.policy.MaximumAttempts; attempt++ {
		// Check if context is canceled
		select {
		case <-ctx.Done():
			stats.LastError = ctx.Err()
			stats.TotalTime = time.Since(startTime)
			return stats, ctx.Err()
		default:
		}

		stats.TotalAttempts = attempt

		// Execute function
		err := fn(ctx)
		if err == nil {
			stats.SuccessfulRetry = true
			stats.TotalTime = time.Since(startTime)
			return stats, nil
		}

		stats.LastError = err

		// Check if error is retryable
		if !IsRetryableError(r.policy, err) {
			break
		}

		// Check if we have more attempts
		if attempt >= r.policy.MaximumAttempts {
			break
		}

		// Calculate retry delay
		delay := CalculateRetryDelay(r.policy, attempt+1)

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			stats.LastError = ctx.Err()
			stats.TotalTime = time.Since(startTime)
			return stats, ctx.Err()
		case <-time.After(delay):
			// Continue to next retry
		}
	}

	stats.TotalTime = time.Since(startTime)
	return stats, stats.LastError
}
