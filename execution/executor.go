package execution

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
)

// Executor handles execution with policies
type Executor struct {
	manager *Manager
	tracker *FailureTracker
	timeout *TimeoutTracker
}

// NewExecutor creates a new execution executor
func NewExecutor(manager *Manager) *Executor {
	return &Executor{
		manager: manager,
		tracker: NewFailureTracker(),
		timeout: NewTimeoutTracker(),
	}
}

// Execute executes a function with the given policy
func (e *Executor) Execute(ctx context.Context, executionID string, policyName string, fn func() error) *ExecutionResult {
	startTime := time.Now()

	// Get execution policy
	policy, exists := e.manager.GetPolicy(policyName)
	if !exists {
		policy = DefaultPolicy()
	}

	// Create execution context
	execCtx := &Context{
		ExecutionID:   executionID,
		AttemptNumber: 1,
		StartTime:     startTime,
		Policy:        policy,
		Metadata:      make(map[string]interface{}),
	}

	// Execute with retry logic
	var lastErr error
	for attempt := int32(1); attempt <= policy.MaxFailures; attempt++ {
		execCtx.AttemptNumber = attempt

		// Execute function with timeout
		result := e.executeWithTimeout(ctx, fn, policy.TimeoutPolicy.WorkflowExecutionTimeout)
		result.AttemptNumber = attempt
		result.ExecutionTime = time.Since(startTime)

		// Handle result
		if result.Success {
			e.manager.HandleResult(execCtx, result)
			return result
		}

		lastErr = result.Error

		// Track failure
		if result.Error != nil {
			e.tracker.TrackFailure(
				fmt.Sprintf("%T", result.Error),
				executionID,
				attempt >= policy.MaxFailures,
			)
		}

		// Check if we should stop
		if policy.StopOnFailure {
			break
		}

		// Apply retry policy
		if attempt < policy.MaxFailures {
			result.RetryScheduled = true
			e.manager.HandleResult(execCtx, result)

			// Wait before retry
			if policy.RetryPolicy != nil {
				waitTime := e.calculateRetryWait(attempt, policy.RetryPolicy)
				time.Sleep(waitTime)
			}
		}
	}

	// Final failure result
	result := &ExecutionResult{
		Success:       false,
		Error:         lastErr,
		ExecutionTime: time.Since(startTime),
		AttemptNumber: execCtx.AttemptNumber,
		FailureReason: "Max retries exceeded",
	}

	e.manager.HandleResult(execCtx, result)
	return result
}

// executeWithTimeout executes function with timeout
func (e *Executor) executeWithTimeout(ctx context.Context, fn func() error, timeout time.Duration) *ExecutionResult {
	result := &ExecutionResult{
		Success: false,
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute in goroutine
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	// Wait for completion or timeout
	select {
	case err := <-done:
		if err != nil {
			result.Error = err
		} else {
			result.Success = true
		}
	case <-timeoutCtx.Done():
		result.Error = fmt.Errorf("execution timeout after %v", timeout)
		result.TimeoutOccurred = true
		e.timeout.TrackTimeout("workflow", timeout, timeout)
	}

	return result
}

// calculateRetryWait calculates wait time for retry based on policy
func (e *Executor) calculateRetryWait(attempt int32, policy *temporal.RetryPolicy) time.Duration {
	if policy == nil {
		return time.Second
	}

	wait := policy.InitialInterval
	for i := int32(1); i < attempt; i++ {
		wait = time.Duration(float64(wait) * policy.BackoffCoefficient)
		if wait > policy.MaximumInterval {
			wait = policy.MaximumInterval
			break
		}
	}

	return wait
}

// ExecuteAsync executes function asynchronously
func (e *Executor) ExecuteAsync(ctx context.Context, executionID string, policyName string, fn func() error) <-chan *ExecutionResult {
	resultCh := make(chan *ExecutionResult, 1)

	go func() {
		defer close(resultCh)
		result := e.Execute(ctx, executionID, policyName, fn)
		resultCh <- result
	}()

	return resultCh
}

// GetFailureTracker returns the failure tracker
func (e *Executor) GetFailureTracker() *FailureTracker {
	return e.tracker
}

// GetTimeoutTracker returns the timeout tracker
func (e *Executor) GetTimeoutTracker() *TimeoutTracker {
	return e.timeout
}

// Utility functions for common execution patterns

// ExecuteWithStrictPolicy executes with strict policy (fail fast)
func ExecuteWithStrictPolicy(ctx context.Context, fn func() error) *ExecutionResult {
	executor := NewExecutor(NewManager())
	executor.manager.RegisterPolicy("strict", StrictPolicy())
	return executor.Execute(ctx, "strict-exec", "strict", fn)
}

// ExecuteWithResilientPolicy executes with resilient policy (continue on failure)
func ExecuteWithResilientPolicy(ctx context.Context, fn func() error) *ExecutionResult {
	executor := NewExecutor(NewManager())
	executor.manager.RegisterPolicy("resilient", ResilientPolicy())
	return executor.Execute(ctx, "resilient-exec", "resilient", fn)
}

// ExecuteWithRetry executes with retry policy
func ExecuteWithRetry(ctx context.Context, maxRetries int32, fn func() error) *ExecutionResult {
	policy := DefaultPolicy()
	policy.MaxFailures = maxRetries
	policy.FailurePolicy = RetryOnFailure

	executor := NewExecutor(NewManager())
	executor.manager.RegisterPolicy("retry", policy)
	return executor.Execute(ctx, "retry-exec", "retry", fn)
}

// ExecuteWithTimeout executes with timeout
func ExecuteWithTimeout(ctx context.Context, timeout time.Duration, fn func() error) *ExecutionResult {
	policy := DefaultPolicy()
	policy.TimeoutPolicy.WorkflowExecutionTimeout = timeout

	executor := NewExecutor(NewManager())
	executor.manager.RegisterPolicy("timeout", policy)
	return executor.Execute(ctx, "timeout-exec", "timeout", fn)
}

// Batch execution utilities

// BatchExecutor executes multiple functions with policies
type BatchExecutor struct {
	executor *Executor
	policy   *Policy
}

// NewBatchExecutor creates a new batch executor
func NewBatchExecutor(policy *Policy) *BatchExecutor {
	manager := NewManager()
	manager.RegisterPolicy("batch", policy)

	return &BatchExecutor{
		executor: NewExecutor(manager),
		policy:   policy,
	}
}

// ExecuteAll executes all functions
func (b *BatchExecutor) ExecuteAll(ctx context.Context, functions []func() error) []*ExecutionResult {
	results := make([]*ExecutionResult, len(functions))

	for i, fn := range functions {
		executionID := fmt.Sprintf("batch-%d", i)
		results[i] = b.executor.Execute(ctx, executionID, "batch", fn)
	}

	return results
}

// ExecuteConcurrent executes all functions concurrently
func (b *BatchExecutor) ExecuteConcurrent(ctx context.Context, functions []func() error) []*ExecutionResult {
	results := make([]*ExecutionResult, len(functions))
	resultChans := make([]<-chan *ExecutionResult, len(functions))

	// Start all executions
	for i, fn := range functions {
		executionID := fmt.Sprintf("concurrent-%d", i)
		resultChans[i] = b.executor.ExecuteAsync(ctx, executionID, "batch", fn)
	}

	// Collect results
	for i, ch := range resultChans {
		results[i] = <-ch
	}

	return results
}

// ExecuteWithLimit executes with concurrency limit
func (b *BatchExecutor) ExecuteWithLimit(ctx context.Context, functions []func() error, limit int) []*ExecutionResult {
	if limit <= 0 || limit >= len(functions) {
		return b.ExecuteConcurrent(ctx, functions)
	}

	results := make([]*ExecutionResult, len(functions))
	semaphore := make(chan struct{}, limit)
	resultChans := make([]<-chan *ExecutionResult, len(functions))

	// Start executions with limit
	for i, fn := range functions {
		go func(index int, function func() error) {
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			executionID := fmt.Sprintf("limited-%d", index)
			resultChans[index] = b.executor.ExecuteAsync(ctx, executionID, "batch", function)
		}(i, fn)
	}

	// Collect results
	for i, ch := range resultChans {
		results[i] = <-ch
	}

	return results
}
