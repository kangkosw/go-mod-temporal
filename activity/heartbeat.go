package activity

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

// HeartbeatManager manages heartbeat functionality for long-running activities
type HeartbeatManager struct {
	ctx               context.Context
	interval          time.Duration
	stopCh            chan struct{}
	lastHeartbeatTime time.Time
}

// NewHeartbeatManager creates a new heartbeat manager
func NewHeartbeatManager(ctx context.Context, interval time.Duration) *HeartbeatManager {
	return &HeartbeatManager{
		ctx:      ctx,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start starts automatic heartbeat
func (h *HeartbeatManager) Start() {
	go h.heartbeatLoop()
}

// Stop stops automatic heartbeat
func (h *HeartbeatManager) Stop() {
	close(h.stopCh)
}

// RecordProgress records progress and sends heartbeat
func (h *HeartbeatManager) RecordProgress(progress float64, message string, details ...interface{}) {
	heartbeatData := map[string]interface{}{
		"progress":  progress,
		"message":   message,
		"timestamp": time.Now(),
	}

	if len(details) > 0 {
		heartbeatData["details"] = details
	}

	activity.RecordHeartbeat(h.ctx, heartbeatData)
	h.lastHeartbeatTime = time.Now()
}

// RecordCustomHeartbeat records custom heartbeat data
func (h *HeartbeatManager) RecordCustomHeartbeat(data interface{}) {
	activity.RecordHeartbeat(h.ctx, data)
	h.lastHeartbeatTime = time.Now()
}

// heartbeatLoop runs the automatic heartbeat loop
func (h *HeartbeatManager) heartbeatLoop() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activity.RecordHeartbeat(h.ctx, map[string]interface{}{
				"timestamp": time.Now(),
				"type":      "automatic",
			})
			h.lastHeartbeatTime = time.Now()
		case <-h.stopCh:
			return
		case <-h.ctx.Done():
			return
		}
	}
}

// GetLastHeartbeatTime returns the last heartbeat time
func (h *HeartbeatManager) GetLastHeartbeatTime() time.Time {
	return h.lastHeartbeatTime
}

// HeartbeatHelper provides helper functions for heartbeat management
type HeartbeatHelper struct{}

// NewHeartbeatHelper creates a new heartbeat helper
func NewHeartbeatHelper() *HeartbeatHelper {
	return &HeartbeatHelper{}
}

// WithProgressTracking wraps activity execution with progress tracking
func (h *HeartbeatHelper) WithProgressTracking(ctx context.Context, totalSteps int, fn func(recordProgress func(step int, message string)) error) error {
	recordProgress := func(step int, message string) {
		_ = float64(step) / float64(totalSteps) * 100 // Calculate progress percentage
		RecordProgress(ctx, step, totalSteps, message)
	}

	return fn(recordProgress)
}

// WithPeriodicHeartbeat wraps activity execution with periodic heartbeat
func (h *HeartbeatHelper) WithPeriodicHeartbeat(ctx context.Context, interval time.Duration, fn func() error) error {
	manager := NewHeartbeatManager(ctx, interval)
	manager.Start()
	defer manager.Stop()

	return fn()
}

// WithHeartbeatTimeout wraps activity execution with heartbeat timeout handling
func (h *HeartbeatHelper) WithHeartbeatTimeout(ctx context.Context, timeout time.Duration, fn func() error) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	ticker := time.NewTicker(timeout / 4) // Send heartbeat every 1/4 of timeout
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			return err
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, map[string]interface{}{
				"timestamp": time.Now(),
				"type":      "timeout_prevention",
			})
		case <-timeoutCtx.Done():
			return fmt.Errorf("activity heartbeat timeout: %w", timeoutCtx.Err())
		}
	}
}

// RestartableActivity provides functionality for restartable activities
type RestartableActivity struct {
	ctx context.Context
}

// NewRestartableActivity creates a new restartable activity
func NewRestartableActivity(ctx context.Context) *RestartableActivity {
	return &RestartableActivity{ctx: ctx}
}

// GetCheckpoint retrieves checkpoint data from previous execution
func (r *RestartableActivity) GetCheckpoint(checkpoint interface{}) (bool, error) {
	if !activity.HasHeartbeatDetails(r.ctx) {
		return false, nil
	}

	err := activity.GetHeartbeatDetails(r.ctx, checkpoint)
	if err != nil {
		return false, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return true, nil
}

// SaveCheckpoint saves checkpoint data for restart capability
func (r *RestartableActivity) SaveCheckpoint(checkpoint interface{}) {
	activity.RecordHeartbeat(r.ctx, checkpoint)
}

// Execute executes activity with restart capability
func (r *RestartableActivity) Execute(fn func(checkpoint interface{}, saveCheckpoint func(interface{})) error, checkpointType interface{}) error {
	// Try to get previous checkpoint
	hasCheckpoint, err := r.GetCheckpoint(checkpointType)
	if err != nil {
		return fmt.Errorf("failed to retrieve checkpoint: %w", err)
	}

	var checkpoint interface{}
	if hasCheckpoint {
		checkpoint = checkpointType
	}

	saveCheckpoint := func(data interface{}) {
		r.SaveCheckpoint(data)
	}

	return fn(checkpoint, saveCheckpoint)
}

// Utility functions for common heartbeat patterns

// SimpleHeartbeat sends a simple heartbeat with message
func SimpleHeartbeat(ctx context.Context, message string) {
	activity.RecordHeartbeat(ctx, map[string]interface{}{
		"message":   message,
		"timestamp": time.Now(),
	})
}

// ProgressHeartbeat sends heartbeat with progress information
func ProgressHeartbeat(ctx context.Context, progress float64, message string) {
	RecordProgress(ctx, int(progress), 100, message)
}

// TimestampHeartbeat sends heartbeat with timestamp
func TimestampHeartbeat(ctx context.Context) {
	activity.RecordHeartbeat(ctx, map[string]interface{}{
		"timestamp": time.Now(),
	})
}

// DetailedHeartbeat sends heartbeat with detailed information
func DetailedHeartbeat(ctx context.Context, data map[string]interface{}) {
	data["timestamp"] = time.Now()
	activity.RecordHeartbeat(ctx, data)
}
