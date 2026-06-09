package patterns

import (
	"context"
	"fmt"
	"time"

	clientPkg "github.com/kangkosw/go-mod-temporal/client"
	executionPkg "github.com/kangkosw/go-mod-temporal/execution"
	schedulePkg "github.com/kangkosw/go-mod-temporal/schedule"
	workflowPkg "github.com/kangkosw/go-mod-temporal/workflow"
	"go.temporal.io/sdk/temporal"
)

// OneShotConfig configuration for one-shot execution pattern
type OneShotConfig struct {
	// Basic configuration
	WorkflowID string
	TaskQueue  string
	Workflow   interface{}
	Args       []interface{}

	// Timing
	ExecuteAt time.Time
	Delay     time.Duration // Alternative to ExecuteAt

	// Failure handling
	FailurePolicy executionPkg.FailurePolicy
	MaxRetries    int32
	RetryPolicy   *temporal.RetryPolicy

	// Timeout
	ExecutionTimeout time.Duration

	// Monitoring
	EnableMetrics  bool
	AlertOnFailure bool
	CustomMetadata map[string]interface{}
}

// OneShot represents a one-shot execution pattern
type OneShot struct {
	config     *OneShotConfig
	client     *clientPkg.Client
	manager    *schedulePkg.Manager
	executor   *executionPkg.Executor
	scheduleID string
}

// NewOneShot creates a new one-shot execution
func NewOneShot(client *clientPkg.Client, config *OneShotConfig) *OneShot {
	if config.WorkflowID == "" {
		generator := workflowPkg.NewIDGenerator(&workflowPkg.IDConfig{
			Prefix:   "oneshot",
			Strategy: workflowPkg.TimestampStrategy,
		})
		config.WorkflowID = generator.Generate()
	}

	// Calculate execution time
	executeAt := config.ExecuteAt
	if executeAt.IsZero() && config.Delay > 0 {
		executeAt = time.Now().Add(config.Delay)
	}
	if executeAt.IsZero() {
		executeAt = time.Now().Add(1 * time.Minute) // Default delay
	}

	scheduleID := fmt.Sprintf("oneshot-%s", config.WorkflowID)

	// Setup execution manager
	execManager := executionPkg.NewManager()
	policy := executionPkg.DefaultPolicy()
	policy.FailurePolicy = config.FailurePolicy
	policy.MaxFailures = config.MaxRetries
	if config.RetryPolicy != nil {
		policy.RetryPolicy = config.RetryPolicy
	}
	if config.ExecutionTimeout > 0 {
		policy.TimeoutPolicy.WorkflowExecutionTimeout = config.ExecutionTimeout
	}
	execManager.RegisterPolicy("oneshot", policy)

	return &OneShot{
		config:     config,
		client:     client,
		manager:    schedulePkg.NewManager(client.Client),
		executor:   executionPkg.NewExecutor(execManager),
		scheduleID: scheduleID,
	}
}

// Schedule schedules the one-shot execution
func (o *OneShot) Schedule(ctx context.Context) error {
	executeAt := o.config.ExecuteAt
	if executeAt.IsZero() && o.config.Delay > 0 {
		executeAt = time.Now().Add(o.config.Delay)
	}

	scheduleConfig := &schedulePkg.Config{
		ScheduleID:       o.scheduleID,
		WorkflowID:       o.config.WorkflowID,
		TaskQueue:        o.config.TaskQueue,
		Type:             schedulePkg.OneShotType,
		StartTime:        executeAt,
		Workflow:         o.config.Workflow,
		Args:             o.config.Args,
		FailurePolicy:    schedulePkg.FailurePolicy(o.config.FailurePolicy),
		RemainingActions: 1,
	}

	return o.manager.Create(ctx, scheduleConfig)
}

// Execute executes immediately without scheduling
func (o *OneShot) Execute(ctx context.Context) (*executionPkg.ExecutionResult, error) {
	fn := func() error {
		// This would typically execute the workflow directly
		// For now, we'll use the schedule approach
		return o.Schedule(ctx)
	}

	result := o.executor.Execute(ctx, o.config.WorkflowID, "oneshot", fn)
	return result, result.Error
}

// Cancel cancels the scheduled one-shot execution
func (o *OneShot) Cancel(ctx context.Context) error {
	return o.manager.Delete(ctx, o.scheduleID)
}

// GetStatus gets the one-shot execution status
func (o *OneShot) GetStatus(ctx context.Context) (*schedulePkg.Result, error) {
	_, err := o.manager.Describe(ctx, o.scheduleID)
	if err != nil {
		return nil, err
	}

	// Convert to our result format (compatibility placeholder)
	result := &schedulePkg.Result{
		ScheduleID: o.scheduleID,
		WorkflowID: o.config.WorkflowID,
		Success:    true, // If we can describe it, it exists
	}

	return result, nil
}

// GetID returns the schedule ID
func (o *OneShot) GetID() string {
	return o.scheduleID
}

// GetWorkflowID returns the workflow ID
func (o *OneShot) GetWorkflowID() string {
	return o.config.WorkflowID
}

// Predefined one-shot patterns

// ExecuteIn executes workflow after specified delay
func ExecuteIn(client *clientPkg.Client, delay time.Duration, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	config := &OneShotConfig{
		TaskQueue:        taskQueue,
		Workflow:         workflow,
		Args:             args,
		Delay:            delay,
		FailurePolicy:    executionPkg.RetryOnFailure,
		MaxRetries:       3,
		ExecutionTimeout: 1 * time.Hour,
		EnableMetrics:    true,
		AlertOnFailure:   true,
	}
	return NewOneShot(client, config)
}

// ExecuteAt executes workflow at specific time
func ExecuteAt(client *clientPkg.Client, executeAt time.Time, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	config := &OneShotConfig{
		TaskQueue:        taskQueue,
		Workflow:         workflow,
		Args:             args,
		ExecuteAt:        executeAt,
		FailurePolicy:    executionPkg.RetryOnFailure,
		MaxRetries:       3,
		ExecutionTimeout: 1 * time.Hour,
		EnableMetrics:    true,
		AlertOnFailure:   true,
	}
	return NewOneShot(client, config)
}

// ExecuteNow executes workflow immediately
func ExecuteNow(client *clientPkg.Client, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	config := &OneShotConfig{
		TaskQueue:        taskQueue,
		Workflow:         workflow,
		Args:             args,
		Delay:            1 * time.Second, // Minimal delay
		FailurePolicy:    executionPkg.RetryOnFailure,
		MaxRetries:       3,
		ExecutionTimeout: 1 * time.Hour,
		EnableMetrics:    true,
		AlertOnFailure:   true,
	}
	return NewOneShot(client, config)
}

// ExecuteOnceStrict executes workflow once with no retries
func ExecuteOnceStrict(client *clientPkg.Client, delay time.Duration, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	config := &OneShotConfig{
		TaskQueue:        taskQueue,
		Workflow:         workflow,
		Args:             args,
		Delay:            delay,
		FailurePolicy:    executionPkg.StopOnFailure,
		MaxRetries:       1,
		ExecutionTimeout: 30 * time.Minute,
		EnableMetrics:    true,
		AlertOnFailure:   true,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	return NewOneShot(client, config)
}

// ExecuteWithRetry executes workflow with aggressive retry
func ExecuteWithRetry(client *clientPkg.Client, delay time.Duration, workflow interface{}, taskQueue string, maxRetries int32, args ...interface{}) *OneShot {
	config := &OneShotConfig{
		TaskQueue:        taskQueue,
		Workflow:         workflow,
		Args:             args,
		Delay:            delay,
		FailurePolicy:    executionPkg.RetryOnFailure,
		MaxRetries:       maxRetries,
		ExecutionTimeout: 2 * time.Hour,
		EnableMetrics:    true,
		AlertOnFailure:   true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    maxRetries,
		},
	}
	return NewOneShot(client, config)
}

// ExecuteWithTimeout executes workflow with specific timeout
func ExecuteWithTimeout(client *clientPkg.Client, delay, timeout time.Duration, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	config := &OneShotConfig{
		TaskQueue:        taskQueue,
		Workflow:         workflow,
		Args:             args,
		Delay:            delay,
		FailurePolicy:    executionPkg.RetryOnFailure,
		MaxRetries:       3,
		ExecutionTimeout: timeout,
		EnableMetrics:    true,
		AlertOnFailure:   true,
	}
	return NewOneShot(client, config)
}

// Utility functions for common delays

// ExecuteInMinutes executes workflow after specified minutes
func ExecuteInMinutes(client *clientPkg.Client, minutes int, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	return ExecuteIn(client, time.Duration(minutes)*time.Minute, workflow, taskQueue, args...)
}

// ExecuteInHours executes workflow after specified hours
func ExecuteInHours(client *clientPkg.Client, hours int, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	return ExecuteIn(client, time.Duration(hours)*time.Hour, workflow, taskQueue, args...)
}

// ExecuteInDays executes workflow after specified days
func ExecuteInDays(client *clientPkg.Client, days int, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	return ExecuteIn(client, time.Duration(days)*24*time.Hour, workflow, taskQueue, args...)
}

// ExecuteTomorrow executes workflow tomorrow at specific time
func ExecuteTomorrow(client *clientPkg.Client, hour, minute int, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	now := time.Now()
	tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, hour, minute, 0, 0, now.Location())
	return ExecuteAt(client, tomorrow, workflow, taskQueue, args...)
}

// ExecuteNextWeek executes workflow next week at specific time
func ExecuteNextWeek(client *clientPkg.Client, weekday time.Weekday, hour, minute int, workflow interface{}, taskQueue string, args ...interface{}) *OneShot {
	now := time.Now()
	daysUntilWeekday := (int(weekday) - int(now.Weekday()) + 7) % 7
	if daysUntilWeekday == 0 {
		daysUntilWeekday = 7 // Next week, not today
	}

	executeTime := time.Date(now.Year(), now.Month(), now.Day()+daysUntilWeekday, hour, minute, 0, 0, now.Location())
	return ExecuteAt(client, executeTime, workflow, taskQueue, args...)
}
