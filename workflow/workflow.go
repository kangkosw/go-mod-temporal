package workflow

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// Options holds workflow execution options
type Options struct {
	// Required fields
	WorkflowID string
	TaskQueue  string

	// Optional fields
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	WorkflowIDReusePolicy    enums.WorkflowIdReusePolicy
	RetryPolicy              *temporal.RetryPolicy
	CronSchedule             string
	Memo                     map[string]interface{}
	SearchAttributes         map[string]interface{}

	// ID generation config
	IDConfig *IDConfig
}

// ExecutionRequest holds workflow execution request
type ExecutionRequest struct {
	Options  *Options
	Workflow interface{}
	Args     []interface{}
}

// ExecutionResult holds workflow execution result
type ExecutionResult struct {
	WorkflowID string
	RunID      string
	Execution  client.WorkflowRun
}

// Manager provides workflow management functionality
type Manager struct {
	client      client.Client
	idGenerator *IDGenerator
}

// NewManager creates a new workflow manager
func NewManager(client client.Client) *Manager {
	return &Manager{
		client:      client,
		idGenerator: NewIDGenerator(nil),
	}
}

// Execute executes a workflow with the given options
func (m *Manager) Execute(ctx context.Context, req *ExecutionRequest) (*ExecutionResult, error) {
	if req.Options == nil {
		return nil, ErrInvalidOptions
	}

	// Generate WorkflowID if needed
	workflowID := req.Options.WorkflowID
	if workflowID == "" && req.Options.IDConfig != nil {
		generator := NewIDGenerator(req.Options.IDConfig)
		workflowID = generator.Generate()
	}

	if err := Validate(workflowID); err != nil {
		return nil, err
	}

	// Build client options
	clientOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: req.Options.TaskQueue,
	}

	if req.Options.WorkflowExecutionTimeout > 0 {
		clientOptions.WorkflowExecutionTimeout = req.Options.WorkflowExecutionTimeout
	}

	if req.Options.WorkflowRunTimeout > 0 {
		clientOptions.WorkflowRunTimeout = req.Options.WorkflowRunTimeout
	}

	if req.Options.WorkflowTaskTimeout > 0 {
		clientOptions.WorkflowTaskTimeout = req.Options.WorkflowTaskTimeout
	}

	if req.Options.WorkflowIDReusePolicy != 0 {
		clientOptions.WorkflowIDReusePolicy = req.Options.WorkflowIDReusePolicy
	}

	if req.Options.RetryPolicy != nil {
		clientOptions.RetryPolicy = req.Options.RetryPolicy
	}

	if req.Options.CronSchedule != "" {
		clientOptions.CronSchedule = req.Options.CronSchedule
	}

	if req.Options.Memo != nil {
		clientOptions.Memo = req.Options.Memo
	}

	if req.Options.SearchAttributes != nil {
		clientOptions.SearchAttributes = req.Options.SearchAttributes
	}

	// Execute workflow
	workflowRun, err := m.client.ExecuteWorkflow(ctx, clientOptions, req.Workflow, req.Args...)
	if err != nil {
		return nil, err
	}

	return &ExecutionResult{
		WorkflowID: workflowID,
		RunID:      workflowRun.GetRunID(),
		Execution:  workflowRun,
	}, nil
}

// GetWorkflow gets workflow execution
func (m *Manager) GetWorkflow(ctx context.Context, workflowID, runID string) client.WorkflowRun {
	return m.client.GetWorkflow(ctx, workflowID, runID)
}

// CancelWorkflow cancels a workflow execution
func (m *Manager) CancelWorkflow(ctx context.Context, workflowID, runID string) error {
	return m.client.CancelWorkflow(ctx, workflowID, runID)
}

// TerminateWorkflow terminates a workflow execution
func (m *Manager) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details ...interface{}) error {
	return m.client.TerminateWorkflow(ctx, workflowID, runID, reason, details...)
}

// SignalWorkflow sends a signal to a workflow
func (m *Manager) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error {
	return m.client.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
}

// SignalWithStartWorkflow signals a workflow or starts it if not running
func (m *Manager) SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	return m.client.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
}

// QueryWorkflow queries a workflow
func (m *Manager) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (interface{}, error) {
	return m.client.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
}

// ListWorkflows lists workflow executions
func (m *Manager) ListWorkflows(ctx context.Context, request interface{}) (interface{}, error) {
	// Note: ListWorkflow API may vary between SDK versions
	// This is a placeholder implementation
	return nil, fmt.Errorf("ListWorkflows not implemented in this SDK version")
}

// DefaultOptions returns default workflow options
func DefaultOptions(workflowID, taskQueue string) *Options {
	return &Options{
		WorkflowID:               workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 24 * time.Hour,
		WorkflowRunTimeout:       24 * time.Hour,
		WorkflowTaskTimeout:      10 * time.Second,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
}
