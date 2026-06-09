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

// CronJobConfig configuration for cron job pattern
type CronJobConfig struct {
	// Basic configuration
	WorkflowID string
	TaskQueue  string
	Schedule   string // Cron expression
	Workflow   interface{}
	Args       []interface{}

	// Execution options
	StartTime     time.Time
	EndTime       time.Time
	MaxExecutions int32

	// Failure handling
	FailurePolicy executionPkg.FailurePolicy
	StopOnFailure bool
	MaxFailures   int32
	RetryPolicy   *temporal.RetryPolicy

	// Overlap handling
	OverlapPolicy schedulePkg.OverlapPolicy

	// Monitoring
	EnableMetrics  bool
	AlertOnFailure bool
	CustomMetadata map[string]interface{}
}

// CronJob represents a cron job pattern
type CronJob struct {
	config     *CronJobConfig
	client     *clientPkg.Client
	manager    *schedulePkg.Manager
	executor   *executionPkg.Executor
	scheduleID string
}

// NewCronJob creates a new cron job
func NewCronJob(client *clientPkg.Client, config *CronJobConfig) *CronJob {
	if config.WorkflowID == "" {
		generator := workflowPkg.NewIDGenerator(&workflowPkg.IDConfig{
			Prefix:   "cron-job",
			Strategy: workflowPkg.TimestampStrategy,
		})
		config.WorkflowID = generator.Generate()
	}

	scheduleID := fmt.Sprintf("cron-%s", config.WorkflowID)

	// Setup execution manager
	execManager := executionPkg.NewManager()
	policy := executionPkg.DefaultPolicy()
	policy.FailurePolicy = config.FailurePolicy
	policy.StopOnFailure = config.StopOnFailure
	policy.MaxFailures = config.MaxFailures
	if config.RetryPolicy != nil {
		policy.RetryPolicy = config.RetryPolicy
	}
	execManager.RegisterPolicy("cron", policy)

	return &CronJob{
		config:     config,
		client:     client,
		manager:    schedulePkg.NewManager(client.Client),
		executor:   executionPkg.NewExecutor(execManager),
		scheduleID: scheduleID,
	}
}

// Start starts the cron job
func (c *CronJob) Start(ctx context.Context) error {
	scheduleConfig := &schedulePkg.Config{
		ScheduleID:       c.scheduleID,
		WorkflowID:       c.config.WorkflowID,
		TaskQueue:        c.config.TaskQueue,
		Type:             schedulePkg.CronType,
		CronExpression:   c.config.Schedule,
		Workflow:         c.config.Workflow,
		Args:             c.config.Args,
		StartTime:        c.config.StartTime,
		EndTime:          c.config.EndTime,
		OverlapPolicy:    c.config.OverlapPolicy,
		RemainingActions: c.config.MaxExecutions,
		FailurePolicy:    schedulePkg.FailurePolicy(c.config.FailurePolicy),
		PauseOnFailure:   c.config.StopOnFailure,
	}

	return c.manager.Create(ctx, scheduleConfig)
}

// Stop stops the cron job
func (c *CronJob) Stop(ctx context.Context) error {
	return c.manager.Delete(ctx, c.scheduleID)
}

// Pause pauses the cron job
func (c *CronJob) Pause(ctx context.Context, reason string) error {
	return c.manager.Pause(ctx, c.scheduleID, reason)
}

// Resume resumes the cron job
func (c *CronJob) Resume(ctx context.Context, reason string) error {
	return c.manager.Unpause(ctx, c.scheduleID, reason)
}

// Trigger manually triggers the cron job
func (c *CronJob) Trigger(ctx context.Context) error {
	return c.manager.Trigger(ctx, c.scheduleID, c.config.OverlapPolicy)
}

// GetStatus gets the cron job status
func (c *CronJob) GetStatus(ctx context.Context) (interface{}, error) {
	return c.manager.Describe(ctx, c.scheduleID)
}

// GetID returns the schedule ID
func (c *CronJob) GetID() string {
	return c.scheduleID
}

// GetWorkflowID returns the workflow ID
func (c *CronJob) GetWorkflowID() string {
	return c.config.WorkflowID
}

// Predefined cron job patterns

// DailyCronJob creates a daily cron job
func DailyCronJob(client *clientPkg.Client, workflow interface{}, taskQueue string, hour, minute int, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       fmt.Sprintf("%d %d * * *", minute, hour),
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.RetryOnFailure,
		StopOnFailure:  false,
		MaxFailures:    3,
		OverlapPolicy:  schedulePkg.OverlapPolicySkip,
		EnableMetrics:  true,
		AlertOnFailure: true,
	}
	return NewCronJob(client, config)
}

// HourlyCronJob creates an hourly cron job
func HourlyCronJob(client *clientPkg.Client, workflow interface{}, taskQueue string, minute int, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       fmt.Sprintf("%d * * * *", minute),
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.RetryOnFailure,
		StopOnFailure:  false,
		MaxFailures:    3,
		OverlapPolicy:  schedulePkg.OverlapPolicySkip,
		EnableMetrics:  true,
		AlertOnFailure: true,
	}
	return NewCronJob(client, config)
}

// WeeklyCronJob creates a weekly cron job
func WeeklyCronJob(client *clientPkg.Client, workflow interface{}, taskQueue string, weekday, hour, minute int, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       fmt.Sprintf("%d %d * * %d", minute, hour, weekday),
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.RetryOnFailure,
		StopOnFailure:  false,
		MaxFailures:    3,
		OverlapPolicy:  schedulePkg.OverlapPolicySkip,
		EnableMetrics:  true,
		AlertOnFailure: true,
	}
	return NewCronJob(client, config)
}

// MonthlyCronJob creates a monthly cron job
func MonthlyCronJob(client *clientPkg.Client, workflow interface{}, taskQueue string, day, hour, minute int, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       fmt.Sprintf("%d %d %d * *", minute, hour, day),
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.RetryOnFailure,
		StopOnFailure:  false,
		MaxFailures:    3,
		OverlapPolicy:  schedulePkg.OverlapPolicySkip,
		EnableMetrics:  true,
		AlertOnFailure: true,
	}
	return NewCronJob(client, config)
}

// CustomCronJob creates a cron job with custom cron expression
func CustomCronJob(client *clientPkg.Client, cronExpr string, workflow interface{}, taskQueue string, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       cronExpr,
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.RetryOnFailure,
		StopOnFailure:  false,
		MaxFailures:    3,
		OverlapPolicy:  schedulePkg.OverlapPolicySkip,
		EnableMetrics:  true,
		AlertOnFailure: true,
	}
	return NewCronJob(client, config)
}

// ResilientCronJob creates a cron job that continues despite failures
func ResilientCronJob(client *clientPkg.Client, cronExpr string, workflow interface{}, taskQueue string, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       cronExpr,
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.ContinueOnFailure,
		StopOnFailure:  false,
		MaxFailures:    10,
		OverlapPolicy:  schedulePkg.OverlapPolicyAllow,
		EnableMetrics:  true,
		AlertOnFailure: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    5,
		},
	}
	return NewCronJob(client, config)
}

// StrictCronJob creates a cron job that stops on first failure
func StrictCronJob(client *clientPkg.Client, cronExpr string, workflow interface{}, taskQueue string, args ...interface{}) *CronJob {
	config := &CronJobConfig{
		TaskQueue:      taskQueue,
		Schedule:       cronExpr,
		Workflow:       workflow,
		Args:           args,
		FailurePolicy:  executionPkg.StopOnFailure,
		StopOnFailure:  true,
		MaxFailures:    1,
		OverlapPolicy:  schedulePkg.OverlapPolicySkip,
		EnableMetrics:  true,
		AlertOnFailure: true,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	return NewCronJob(client, config)
}
