package workflow

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// ContextUtils provides simple workflow context utilities
type ContextUtils struct{}

// NewContextUtils creates a new context utilities
func NewContextUtils() *ContextUtils {
	return &ContextUtils{}
}

// Sleep sleeps for the given duration
func (cu *ContextUtils) Sleep(ctx workflow.Context, duration time.Duration) error {
	return workflow.Sleep(ctx, duration)
}

// Now returns current workflow time
func (cu *ContextUtils) Now(ctx workflow.Context) time.Time {
	return workflow.Now(ctx)
}

// GetInfo returns workflow info
func (cu *ContextUtils) GetInfo(ctx workflow.Context) *workflow.Info {
	return workflow.GetInfo(ctx)
}

// IsReplaying checks if workflow is replaying
func (cu *ContextUtils) IsReplaying(ctx workflow.Context) bool {
	return workflow.IsReplaying(ctx)
}

// GetLogger returns workflow logger
func (cu *ContextUtils) GetLogger(ctx workflow.Context) interface{} {
	return workflow.GetLogger(ctx)
}

// CreateTimer creates a workflow timer
func (cu *ContextUtils) CreateTimer(ctx workflow.Context, duration time.Duration) workflow.Future {
	return workflow.NewTimer(ctx, duration)
}

// ExecuteActivity executes an activity
func (cu *ContextUtils) ExecuteActivity(ctx workflow.Context, activity interface{}, args ...interface{}) workflow.Future {
	return workflow.ExecuteActivity(ctx, activity, args...)
}

// SideEffect executes a side effect
func (cu *ContextUtils) SideEffect(ctx workflow.Context, f func(ctx workflow.Context) interface{}) interface{} {
	return workflow.SideEffect(ctx, f)
}

// GetVersion gets workflow version
func (cu *ContextUtils) GetVersion(ctx workflow.Context, changeID string, minSupported, maxSupported int) int {
	return int(workflow.GetVersion(ctx, changeID, workflow.Version(minSupported), workflow.Version(maxSupported)))
}

// ContinueAsNew continues workflow as new
func (cu *ContextUtils) ContinueAsNew(ctx workflow.Context, workflowFunc interface{}, args ...interface{}) error {
	return workflow.NewContinueAsNewError(ctx, workflowFunc, args...)
}