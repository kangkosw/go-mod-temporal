package patterns

import (
	"context"
	"time"

	clientPkg "github.com/kangkosw/go-mod-temporal/client"
	executionPkg "github.com/kangkosw/go-mod-temporal/execution"
	workflowPkg "github.com/kangkosw/go-mod-temporal/workflow"
	"go.temporal.io/sdk/temporal"
)

// RetryConfig configuration for retry execution pattern
type RetryConfig struct {
	// Basic configuration
	WorkflowID string
	TaskQueue  string
	Workflow   interface{}
	Args       []interface{}

	// Retry configuration
	MaxRetries         int32
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration

	// Failure handling
	StopOnFailure      bool
	ContinueOnFailure  bool
	NonRetryableErrors []string

	// Timeout
	ExecutionTimeout time.Duration
	RetryTimeout     time.Duration

	// Monitoring
	EnableMetrics  bool
	AlertOnFailure bool
	AlertThreshold int32
	CustomMetadata map[string]interface{}
}

// RetryPattern represents a retry execution pattern
type RetryPattern struct {
	config   *RetryConfig
	client   *clientPkg.Client
	executor *executionPkg.Executor
	manager  *workflowPkg.Manager
}

// NewRetryPattern creates a new retry pattern
func NewRetryPattern(client *clientPkg.Client, config *RetryConfig) *RetryPattern {
	if config.WorkflowID == "" {
		generator := workflowPkg.NewIDGenerator(&workflowPkg.IDConfig{
			Prefix:   "retry",
			Strategy: workflowPkg.TimestampStrategy,
		})
		config.WorkflowID = generator.Generate()
	}

	// Setup execution manager
	execManager := executionPkg.NewManager()
	policy := &executionPkg.Policy{
		FailurePolicy:     executionPkg.RetryOnFailure,
		MaxFailures:       config.MaxRetries,
		StopOnFailure:     config.StopOnFailure,
		ContinueOnFailure: config.ContinueOnFailure,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        config.InitialInterval,
			BackoffCoefficient:     config.BackoffCoefficient,
			MaximumInterval:        config.MaxInterval,
			MaximumAttempts:        config.MaxRetries,
			NonRetryableErrorTypes: config.NonRetryableErrors,
		},
		TimeoutPolicy: executionPkg.TimeoutPolicy{
			WorkflowExecutionTimeout: config.ExecutionTimeout,
		},
		AlertOnFailure: config.AlertOnFailure,
		AlertThreshold: config.AlertThreshold,
		MetricsEnabled: config.EnableMetrics,
	}
	execManager.RegisterPolicy("retry", policy)

	return &RetryPattern{
		config:   config,
		client:   client,
		executor: executionPkg.NewExecutor(execManager),
		manager:  workflowPkg.NewManager(client.Client),
	}
}

// Execute executes the workflow with retry pattern
func (r *RetryPattern) Execute(ctx context.Context) (*executionPkg.ExecutionResult, error) {
	// Create execution function
	fn := func() error {
		// Execute workflow using workflow manager
		request := &workflowPkg.ExecutionRequest{
			Options: &workflowPkg.Options{
				WorkflowID:               r.config.WorkflowID,
				TaskQueue:                r.config.TaskQueue,
				WorkflowExecutionTimeout: r.config.ExecutionTimeout,
				RetryPolicy:              nil, // RetryPolicy needs proper temporal.RetryPolicy type
			},
			Workflow: r.config.Workflow,
			Args:     r.config.Args,
		}

		_, err := r.manager.Execute(ctx, request)
		return err
	}

	// Execute with retry
	result := r.executor.Execute(ctx, r.config.WorkflowID, "retry", fn)
	return result, result.Error
}

// ExecuteAsync executes the workflow asynchronously with retry pattern
func (r *RetryPattern) ExecuteAsync(ctx context.Context) <-chan *executionPkg.ExecutionResult {
	fn := func() error {
		request := &workflowPkg.ExecutionRequest{
			Options: &workflowPkg.Options{
				WorkflowID:               r.config.WorkflowID,
				TaskQueue:                r.config.TaskQueue,
				WorkflowExecutionTimeout: r.config.ExecutionTimeout,
			},
			Workflow: r.config.Workflow,
			Args:     r.config.Args,
		}

		_, err := r.manager.Execute(ctx, request)
		return err
	}

	return r.executor.ExecuteAsync(ctx, r.config.WorkflowID, "retry", fn)
}

// GetWorkflowID returns the workflow ID
func (r *RetryPattern) GetWorkflowID() string {
	return r.config.WorkflowID
}

// GetConfig returns the retry configuration
func (r *RetryPattern) GetConfig() *RetryConfig {
	return r.config
}

// Predefined retry patterns

// QuickRetry creates a retry pattern with quick retries
func QuickRetry(client *clientPkg.Client, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         5,
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 1.5,
		MaxInterval:        5 * time.Second,
		StopOnFailure:      false,
		ExecutionTimeout:   1 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     3,
	}
	return NewRetryPattern(client, config)
}

// StandardRetry creates a retry pattern with standard retry intervals
func StandardRetry(client *clientPkg.Client, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         3,
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 2.0,
		MaxInterval:        30 * time.Second,
		StopOnFailure:      false,
		ExecutionTimeout:   10 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     2,
	}
	return NewRetryPattern(client, config)
}

// AggressiveRetry creates a retry pattern with aggressive retries
func AggressiveRetry(client *clientPkg.Client, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         10,
		InitialInterval:    50 * time.Millisecond,
		BackoffCoefficient: 1.2,
		MaxInterval:        2 * time.Second,
		StopOnFailure:      false,
		ExecutionTimeout:   5 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     5,
	}
	return NewRetryPattern(client, config)
}

// PatientRetry creates a retry pattern with long intervals for external dependencies
func PatientRetry(client *clientPkg.Client, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         5,
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 2.0,
		MaxInterval:        5 * time.Minute,
		StopOnFailure:      false,
		ExecutionTimeout:   30 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     3,
	}
	return NewRetryPattern(client, config)
}

// StrictRetry creates a retry pattern that stops on first failure
func StrictRetry(client *clientPkg.Client, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         1,
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 1.0,
		MaxInterval:        1 * time.Second,
		StopOnFailure:      true,
		ExecutionTimeout:   5 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     1,
	}
	return NewRetryPattern(client, config)
}

// CustomRetry creates a retry pattern with custom configuration
func CustomRetry(client *clientPkg.Client, maxRetries int32, initialInterval, maxInterval time.Duration, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         maxRetries,
		InitialInterval:    initialInterval,
		BackoffCoefficient: 2.0,
		MaxInterval:        maxInterval,
		StopOnFailure:      false,
		ExecutionTimeout:   30 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     maxRetries / 2,
	}
	return NewRetryPattern(client, config)
}

// RetryWithNonRetryableErrors creates a retry pattern with specific non-retryable errors
func RetryWithNonRetryableErrors(client *clientPkg.Client, nonRetryableErrors []string, workflow interface{}, taskQueue string, args ...interface{}) *RetryPattern {
	config := &RetryConfig{
		TaskQueue:          taskQueue,
		Workflow:           workflow,
		Args:               args,
		MaxRetries:         3,
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 2.0,
		MaxInterval:        30 * time.Second,
		StopOnFailure:      false,
		NonRetryableErrors: nonRetryableErrors,
		ExecutionTimeout:   10 * time.Minute,
		EnableMetrics:      true,
		AlertOnFailure:     true,
		AlertThreshold:     2,
	}
	return NewRetryPattern(client, config)
}

// Batch retry operations

// BatchRetryConfig configuration for batch retry operations
type BatchRetryConfig struct {
	MaxConcurrency int
	RetryConfig    *RetryConfig
}

// BatchRetryPattern handles multiple workflows with retry
type BatchRetryPattern struct {
	config   *BatchRetryConfig
	client   *clientPkg.Client
	executor *executionPkg.BatchExecutor
}

// NewBatchRetryPattern creates a new batch retry pattern
func NewBatchRetryPattern(client *clientPkg.Client, config *BatchRetryConfig) *BatchRetryPattern {
	// Convert retry config to execution policy
	policy := &executionPkg.Policy{
		FailurePolicy:     executionPkg.RetryOnFailure,
		MaxFailures:       config.RetryConfig.MaxRetries,
		StopOnFailure:     config.RetryConfig.StopOnFailure,
		ContinueOnFailure: config.RetryConfig.ContinueOnFailure,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        config.RetryConfig.InitialInterval,
			BackoffCoefficient:     config.RetryConfig.BackoffCoefficient,
			MaximumInterval:        config.RetryConfig.MaxInterval,
			MaximumAttempts:        config.RetryConfig.MaxRetries,
			NonRetryableErrorTypes: config.RetryConfig.NonRetryableErrors,
		},
		ConcurrencyLimit: int32(config.MaxConcurrency),
	}

	return &BatchRetryPattern{
		config:   config,
		client:   client,
		executor: executionPkg.NewBatchExecutor(policy),
	}
}

// ExecuteAll executes all workflows with retry
func (b *BatchRetryPattern) ExecuteAll(ctx context.Context, workflows []func() error) []*executionPkg.ExecutionResult {
	return b.executor.ExecuteAll(ctx, workflows)
}

// ExecuteConcurrent executes workflows concurrently with retry
func (b *BatchRetryPattern) ExecuteConcurrent(ctx context.Context, workflows []func() error) []*executionPkg.ExecutionResult {
	if b.config.MaxConcurrency > 0 {
		return b.executor.ExecuteWithLimit(ctx, workflows, b.config.MaxConcurrency)
	}
	return b.executor.ExecuteConcurrent(ctx, workflows)
}
