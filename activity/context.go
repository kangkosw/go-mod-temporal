package activity

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
)

// Context wraps activity context with additional utilities
type Context struct {
	context.Context
}

// NewContext creates a new activity context wrapper
func NewContext(ctx context.Context) *Context {
	return &Context{
		Context: ctx,
	}
}

// Info returns activity execution info
type Info struct {
	TaskToken         []byte
	WorkflowType      string
	WorkflowExecution string
	ActivityID        string
	ActivityType      string
	TaskQueue         string
	HeartbeatTimeout  time.Duration
	ScheduledTime     time.Time
	StartedTime       time.Time
	Deadline          time.Time
	Attempt           int32
}

// GetInfo returns activity information
func (c *Context) GetInfo() *Info {
	info := activity.GetInfo(c.Context)
	return &Info{
		TaskToken:         info.TaskToken,
		WorkflowType:      info.WorkflowType.Name,
		WorkflowExecution: info.WorkflowExecution.ID,
		ActivityID:        info.ActivityID,
		ActivityType:      info.ActivityType.Name,
		TaskQueue:         info.TaskQueue,
		HeartbeatTimeout:  info.HeartbeatTimeout,
		ScheduledTime:     info.ScheduledTime,
		StartedTime:       info.StartedTime,
		Deadline:          info.Deadline,
		Attempt:           info.Attempt,
	}
}

// GetLogger returns the activity logger
func (c *Context) GetLogger() interface{} {
	return activity.GetLogger(c.Context)
}

// GetMetricsHandler returns the metrics handler
func (c *Context) GetMetricsHandler() interface{} {
	return activity.GetMetricsHandler(c.Context)
}

// RecordHeartbeat records a heartbeat with optional details
func (c *Context) RecordHeartbeat(details ...interface{}) {
	activity.RecordHeartbeat(c.Context, details...)
}

// RecordHeartbeatWithProgress records heartbeat with progress information
func (c *Context) RecordHeartbeatWithProgress(progress float64, message string) {
	heartbeatData := map[string]interface{}{
		"progress":  progress,
		"message":   message,
		"timestamp": time.Now(),
	}
	activity.RecordHeartbeat(c.Context, heartbeatData)
}

// GetHeartbeatDetails retrieves heartbeat details from previous attempt
func (c *Context) GetHeartbeatDetails(d ...interface{}) error {
	return activity.GetHeartbeatDetails(c.Context, d...)
}

// HasHeartbeatDetails checks if heartbeat details exist
func (c *Context) HasHeartbeatDetails() bool {
	return activity.HasHeartbeatDetails(c.Context)
}

// GetWorkerStopChannel returns a channel that will be closed when the worker is shutting down
func (c *Context) GetWorkerStopChannel() <-chan struct{} {
	return activity.GetWorkerStopChannel(c.Context)
}

// CreateLogger creates a logger with activity information
func (c *Context) CreateLogger(name string) interface{} {
	// Return basic logger for compatibility
	// Enhanced logging with activity info can be added later
	return c.GetLogger()
}

// IsRetry checks if this is a retry attempt
func (c *Context) IsRetry() bool {
	info := c.GetInfo()
	return info.Attempt > 1
}

// GetRetryAttempt returns the current retry attempt number
func (c *Context) GetRetryAttempt() int32 {
	info := c.GetInfo()
	return info.Attempt
}

// GetRemainingTime returns the remaining time before activity deadline
func (c *Context) GetRemainingTime() time.Duration {
	info := c.GetInfo()
	return time.Until(info.Deadline)
}

// ShouldHeartbeat checks if heartbeat should be sent based on timeout
func (c *Context) ShouldHeartbeat() bool {
	info := c.GetInfo()
	if info.HeartbeatTimeout <= 0 {
		return false
	}

	// Send heartbeat if more than 1/3 of heartbeat timeout has passed
	elapsed := time.Since(info.StartedTime)
	threshold := info.HeartbeatTimeout / 3
	return elapsed >= threshold
}

// WithTimeout creates a context with timeout
func (c *Context) WithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Context, timeout)
}

// WithCancel creates a context with cancel
func (c *Context) WithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(c.Context)
}

// WithDeadline creates a context with deadline
func (c *Context) WithDeadline(deadline time.Time) (context.Context, context.CancelFunc) {
	return context.WithDeadline(c.Context, deadline)
}

// Helper functions for activity context

// LogActivityStart logs activity start with standard format
func LogActivityStart(ctx context.Context, args ...interface{}) {
	logger := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	logger.Info("Activity started",
		"ActivityID", info.ActivityID,
		"ActivityType", info.ActivityType.Name,
		"WorkflowExecution", info.WorkflowExecution.ID,
		"TaskQueue", info.TaskQueue,
		"Attempt", info.Attempt,
		"Args", args,
	)
}

// LogActivityComplete logs activity completion with standard format
func LogActivityComplete(ctx context.Context, result interface{}, err error) {
	logger := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)

	if err != nil {
		logger.Error("Activity completed with error",
			"ActivityID", info.ActivityID,
			"ActivityType", info.ActivityType.Name,
			"Attempt", info.Attempt,
			"Error", err,
		)
	} else {
		logger.Info("Activity completed successfully",
			"ActivityID", info.ActivityID,
			"ActivityType", info.ActivityType.Name,
			"Attempt", info.Attempt,
			"Result", result,
		)
	}
}

// RecordProgress records progress with standardized format
func RecordProgress(ctx context.Context, current, total int, message string) {
	progress := float64(current) / float64(total) * 100
	heartbeatData := map[string]interface{}{
		"progress":  progress,
		"current":   current,
		"total":     total,
		"message":   message,
		"timestamp": time.Now(),
	}
	activity.RecordHeartbeat(ctx, heartbeatData)
}
