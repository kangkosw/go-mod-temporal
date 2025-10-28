package execution

import (
	"time"

	"go.temporal.io/sdk/temporal"
)

// Policy defines execution behavior policies
type Policy struct {
	// Failure handling
	FailurePolicy     FailurePolicy
	MaxFailures       int32
	StopOnFailure     bool
	ContinueOnFailure bool

	// Timeout policies
	TimeoutPolicy TimeoutPolicy

	// Retry policies
	RetryPolicy *temporal.RetryPolicy

	// Execution limits
	MaxExecutions    int32
	ExecutionWindow  time.Duration
	ConcurrencyLimit int32

	// Monitoring and alerts
	AlertOnFailure bool
	AlertThreshold int32
	MetricsEnabled bool
}

// FailurePolicy defines how to handle execution failures
type FailurePolicy int

const (
	// StopOnFailure stops execution immediately when failure occurs
	StopOnFailure FailurePolicy = iota
	// ContinueOnFailure continues execution despite failures
	ContinueOnFailure
	// PauseOnFailure pauses execution when failure occurs
	PauseOnFailure
	// RetryOnFailure retries the failed execution
	RetryOnFailure
	// SkipOnFailure skips the failed execution and continues
	SkipOnFailure
	// EscalateOnFailure escalates failure to higher level
	EscalateOnFailure
)

// TimeoutPolicy defines timeout behavior
type TimeoutPolicy struct {
	// Workflow timeouts
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration

	// Activity timeouts
	ActivityScheduleToCloseTimeout time.Duration
	ActivityScheduleToStartTimeout time.Duration
	ActivityStartToCloseTimeout    time.Duration
	ActivityHeartbeatTimeout       time.Duration

	// Schedule timeouts
	ScheduleToCloseTimeout time.Duration

	// Custom timeouts
	CustomTimeouts map[string]time.Duration

	// Timeout actions
	OnWorkflowTimeout OnTimeoutAction
	OnActivityTimeout OnTimeoutAction
}

// OnTimeoutAction defines action to take on timeout
type OnTimeoutAction int

const (
	// TimeoutActionTerminate terminates the execution
	TimeoutActionTerminate OnTimeoutAction = iota
	// TimeoutActionRetry retries the execution
	TimeoutActionRetry
	// TimeoutActionContinue continues with timeout
	TimeoutActionContinue
	// TimeoutActionEscalate escalates the timeout
	TimeoutActionEscalate
	// TimeoutActionCancel cancels the execution
	TimeoutActionCancel
)

// ExecutionResult holds execution result information
type ExecutionResult struct {
	Success         bool
	Error           error
	ExecutionTime   time.Duration
	AttemptNumber   int32
	FailureReason   string
	TimeoutOccurred bool
	RetryScheduled  bool
	Metrics         *ExecutionMetrics
}

// ExecutionMetrics holds execution metrics
type ExecutionMetrics struct {
	StartTime     time.Time
	EndTime       time.Time
	Duration      time.Duration
	AttemptCount  int32
	FailureCount  int32
	TimeoutCount  int32
	RetryCount    int32
	MemoryUsage   int64
	CPUUsage      float64
	CustomMetrics map[string]interface{}
}

// Context holds execution context information
type Context struct {
	ExecutionID   string
	WorkflowID    string
	ActivityID    string
	TaskQueue     string
	AttemptNumber int32
	StartTime     time.Time
	Policy        *Policy
	Metadata      map[string]interface{}
}

// Handler defines execution result handler interface
type Handler interface {
	OnSuccess(ctx *Context, result *ExecutionResult) error
	OnFailure(ctx *Context, result *ExecutionResult) error
	OnTimeout(ctx *Context, result *ExecutionResult) error
	OnRetry(ctx *Context, result *ExecutionResult) error
}

// Manager manages execution policies and results
type Manager struct {
	policies map[string]*Policy
	handlers map[string]Handler
	metrics  *MetricsCollector
}

// NewManager creates a new execution manager
func NewManager() *Manager {
	return &Manager{
		policies: make(map[string]*Policy),
		handlers: make(map[string]Handler),
		metrics:  NewMetricsCollector(),
	}
}

// RegisterPolicy registers an execution policy
func (m *Manager) RegisterPolicy(name string, policy *Policy) {
	m.policies[name] = policy
}

// GetPolicy retrieves an execution policy
func (m *Manager) GetPolicy(name string) (*Policy, bool) {
	policy, exists := m.policies[name]
	return policy, exists
}

// RegisterHandler registers an execution result handler
func (m *Manager) RegisterHandler(name string, handler Handler) {
	m.handlers[name] = handler
}

// GetHandler retrieves an execution result handler
func (m *Manager) GetHandler(name string) (Handler, bool) {
	handler, exists := m.handlers[name]
	return handler, exists
}

// HandleResult processes execution result according to policy
func (m *Manager) HandleResult(ctx *Context, result *ExecutionResult) error {
	// Record metrics
	m.metrics.Record(ctx, result)

	// Get policy for this execution
	policy := ctx.Policy
	if policy == nil {
		policy = DefaultPolicy()
	}

	// Handle based on result and policy
	if result.Success {
		return m.handleSuccess(ctx, result, policy)
	} else {
		return m.handleFailure(ctx, result, policy)
	}
}

// handleSuccess handles successful execution
func (m *Manager) handleSuccess(ctx *Context, result *ExecutionResult, policy *Policy) error {
	// Call success handlers
	if handler, exists := m.handlers[ctx.ExecutionID]; exists {
		return handler.OnSuccess(ctx, result)
	}
	return nil
}

// handleFailure handles failed execution
func (m *Manager) handleFailure(ctx *Context, result *ExecutionResult, policy *Policy) error {
	// Check if we should stop on failure
	if policy.StopOnFailure {
		result.RetryScheduled = false
		if handler, exists := m.handlers[ctx.ExecutionID]; exists {
			return handler.OnFailure(ctx, result)
		}
		return result.Error
	}

	// Apply failure policy
	switch policy.FailurePolicy {
	case StopOnFailure:
		result.RetryScheduled = false
		return m.callFailureHandler(ctx, result)

	case ContinueOnFailure:
		// Continue execution, log failure
		return m.callFailureHandler(ctx, result)

	case PauseOnFailure:
		// Pause and notify
		return m.callFailureHandler(ctx, result)

	case RetryOnFailure:
		// Check if we can retry
		if ctx.AttemptNumber < policy.MaxFailures {
			result.RetryScheduled = true
			if handler, exists := m.handlers[ctx.ExecutionID]; exists {
				return handler.OnRetry(ctx, result)
			}
		} else {
			result.RetryScheduled = false
			return m.callFailureHandler(ctx, result)
		}

	case SkipOnFailure:
		// Skip this execution, continue with next
		return nil

	case EscalateOnFailure:
		// Escalate to higher level
		return m.callFailureHandler(ctx, result)
	}

	return result.Error
}

// callFailureHandler calls the appropriate failure handler
func (m *Manager) callFailureHandler(ctx *Context, result *ExecutionResult) error {
	if handler, exists := m.handlers[ctx.ExecutionID]; exists {
		return handler.OnFailure(ctx, result)
	}
	return result.Error
}

// Predefined policies

// DefaultPolicy returns a default execution policy
func DefaultPolicy() *Policy {
	return &Policy{
		FailurePolicy:     RetryOnFailure,
		MaxFailures:       3,
		StopOnFailure:     false,
		ContinueOnFailure: false,
		TimeoutPolicy: TimeoutPolicy{
			WorkflowExecutionTimeout:       24 * time.Hour,
			WorkflowRunTimeout:             24 * time.Hour,
			WorkflowTaskTimeout:            10 * time.Second,
			ActivityScheduleToCloseTimeout: 10 * time.Minute,
			ActivityStartToCloseTimeout:    5 * time.Minute,
			ActivityHeartbeatTimeout:       30 * time.Second,
			OnWorkflowTimeout:              TimeoutActionRetry,
			OnActivityTimeout:              TimeoutActionRetry,
		},
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
		MaxExecutions:    0, // Unlimited
		ConcurrencyLimit: 10,
		AlertOnFailure:   true,
		AlertThreshold:   3,
		MetricsEnabled:   true,
	}
}

// StrictPolicy returns a strict execution policy (stop on first failure)
func StrictPolicy() *Policy {
	policy := DefaultPolicy()
	policy.FailurePolicy = StopOnFailure
	policy.StopOnFailure = true
	policy.MaxFailures = 1
	policy.RetryPolicy.MaximumAttempts = 1
	return policy
}

// ResilientPolicy returns a resilient execution policy (continue despite failures)
func ResilientPolicy() *Policy {
	policy := DefaultPolicy()
	policy.FailurePolicy = ContinueOnFailure
	policy.ContinueOnFailure = true
	policy.MaxFailures = 10
	policy.RetryPolicy.MaximumAttempts = 5
	policy.RetryPolicy.MaximumInterval = 5 * time.Minute
	return policy
}

// QuickFailPolicy returns a policy that fails quickly
func QuickFailPolicy() *Policy {
	policy := DefaultPolicy()
	policy.FailurePolicy = StopOnFailure
	policy.MaxFailures = 1
	policy.TimeoutPolicy.WorkflowExecutionTimeout = 5 * time.Minute
	policy.TimeoutPolicy.ActivityStartToCloseTimeout = 30 * time.Second
	policy.RetryPolicy.MaximumAttempts = 1
	return policy
}

// AggressiveRetryPolicy returns a policy with aggressive retry
func AggressiveRetryPolicy() *Policy {
	policy := DefaultPolicy()
	policy.FailurePolicy = RetryOnFailure
	policy.MaxFailures = 10
	policy.RetryPolicy.InitialInterval = 100 * time.Millisecond
	policy.RetryPolicy.BackoffCoefficient = 1.5
	policy.RetryPolicy.MaximumInterval = 10 * time.Second
	policy.RetryPolicy.MaximumAttempts = 10
	return policy
}

// LongRunningPolicy returns a policy optimized for long-running processes
func LongRunningPolicy() *Policy {
	policy := DefaultPolicy()
	policy.TimeoutPolicy.WorkflowExecutionTimeout = 7 * 24 * time.Hour // 1 week
	policy.TimeoutPolicy.WorkflowRunTimeout = 7 * 24 * time.Hour
	policy.TimeoutPolicy.ActivityScheduleToCloseTimeout = 1 * time.Hour
	policy.TimeoutPolicy.ActivityStartToCloseTimeout = 30 * time.Minute
	policy.TimeoutPolicy.ActivityHeartbeatTimeout = 5 * time.Minute
	policy.FailurePolicy = RetryOnFailure
	policy.MaxFailures = 5
	return policy
}
