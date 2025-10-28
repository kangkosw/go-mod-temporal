package execution

import (
	"fmt"
	"sync"
	"time"
)

// MetricsCollector collects execution metrics
type MetricsCollector struct {
	metrics map[string]*ExecutionMetrics
	mutex   sync.RWMutex
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]*ExecutionMetrics),
	}
}

// Record records execution metrics
func (c *MetricsCollector) Record(ctx *Context, result *ExecutionResult) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	key := fmt.Sprintf("%s-%s", ctx.WorkflowID, ctx.ExecutionID)

	if existing, exists := c.metrics[key]; exists {
		// Update existing metrics
		existing.EndTime = time.Now()
		existing.Duration = existing.EndTime.Sub(existing.StartTime)
		existing.AttemptCount = ctx.AttemptNumber

		if !result.Success {
			existing.FailureCount++
		}

		if result.TimeoutOccurred {
			existing.TimeoutCount++
		}

		if result.RetryScheduled {
			existing.RetryCount++
		}

		// Update custom metrics
		if result.Metrics != nil {
			if existing.CustomMetrics == nil {
				existing.CustomMetrics = make(map[string]interface{})
			}
			for k, v := range result.Metrics.CustomMetrics {
				existing.CustomMetrics[k] = v
			}
		}
	} else {
		// Create new metrics
		metrics := &ExecutionMetrics{
			StartTime:     ctx.StartTime,
			EndTime:       time.Now(),
			Duration:      time.Since(ctx.StartTime),
			AttemptCount:  ctx.AttemptNumber,
			CustomMetrics: make(map[string]interface{}),
		}

		if !result.Success {
			metrics.FailureCount = 1
		}

		if result.TimeoutOccurred {
			metrics.TimeoutCount = 1
		}

		if result.RetryScheduled {
			metrics.RetryCount = 1
		}

		if result.Metrics != nil {
			metrics.MemoryUsage = result.Metrics.MemoryUsage
			metrics.CPUUsage = result.Metrics.CPUUsage
			for k, v := range result.Metrics.CustomMetrics {
				metrics.CustomMetrics[k] = v
			}
		}

		c.metrics[key] = metrics
	}
}

// Get retrieves metrics for a specific execution
func (c *MetricsCollector) Get(workflowID, executionID string) (*ExecutionMetrics, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	key := fmt.Sprintf("%s-%s", workflowID, executionID)
	metrics, exists := c.metrics[key]
	return metrics, exists
}

// GetAll retrieves all collected metrics
func (c *MetricsCollector) GetAll() map[string]*ExecutionMetrics {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make(map[string]*ExecutionMetrics)
	for k, v := range c.metrics {
		result[k] = v
	}
	return result
}

// Clear clears all metrics
func (c *MetricsCollector) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.metrics = make(map[string]*ExecutionMetrics)
}

// GetSummary returns execution summary statistics
func (c *MetricsCollector) GetSummary() *ExecutionSummary {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	summary := &ExecutionSummary{}

	for _, metrics := range c.metrics {
		summary.TotalExecutions++
		summary.TotalAttempts += int64(metrics.AttemptCount)
		summary.TotalFailures += int64(metrics.FailureCount)
		summary.TotalTimeouts += int64(metrics.TimeoutCount)
		summary.TotalRetries += int64(metrics.RetryCount)
		summary.TotalDuration += metrics.Duration

		if metrics.FailureCount == 0 {
			summary.SuccessfulExecutions++
		}

		if summary.MinDuration == 0 || metrics.Duration < summary.MinDuration {
			summary.MinDuration = metrics.Duration
		}

		if metrics.Duration > summary.MaxDuration {
			summary.MaxDuration = metrics.Duration
		}
	}

	if summary.TotalExecutions > 0 {
		summary.AverageDuration = summary.TotalDuration / time.Duration(summary.TotalExecutions)
		summary.SuccessRate = float64(summary.SuccessfulExecutions) / float64(summary.TotalExecutions) * 100
		summary.FailureRate = float64(summary.TotalFailures) / float64(summary.TotalAttempts) * 100
	}

	return summary
}

// ExecutionSummary holds summary statistics
type ExecutionSummary struct {
	TotalExecutions      int64
	SuccessfulExecutions int64
	TotalAttempts        int64
	TotalFailures        int64
	TotalTimeouts        int64
	TotalRetries         int64
	TotalDuration        time.Duration
	AverageDuration      time.Duration
	MinDuration          time.Duration
	MaxDuration          time.Duration
	SuccessRate          float64
	FailureRate          float64
}

// FailureTracker tracks failure patterns
type FailureTracker struct {
	failures map[string]*FailurePattern
	mutex    sync.RWMutex
}

// FailurePattern represents a failure pattern
type FailurePattern struct {
	ErrorType  string
	Count      int64
	FirstSeen  time.Time
	LastSeen   time.Time
	Frequency  float64
	IsCritical bool
	Contexts   []string
}

// NewFailureTracker creates a new failure tracker
func NewFailureTracker() *FailureTracker {
	return &FailureTracker{
		failures: make(map[string]*FailurePattern),
	}
}

// TrackFailure tracks a failure occurrence
func (t *FailureTracker) TrackFailure(errorType, context string, isCritical bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	pattern, exists := t.failures[errorType]
	if !exists {
		pattern = &FailurePattern{
			ErrorType:  errorType,
			Count:      0,
			FirstSeen:  time.Now(),
			IsCritical: isCritical,
			Contexts:   make([]string, 0),
		}
		t.failures[errorType] = pattern
	}

	pattern.Count++
	pattern.LastSeen = time.Now()

	// Add context if not already present
	contextExists := false
	for _, ctx := range pattern.Contexts {
		if ctx == context {
			contextExists = true
			break
		}
	}
	if !contextExists {
		pattern.Contexts = append(pattern.Contexts, context)
	}

	// Calculate frequency (failures per hour)
	duration := pattern.LastSeen.Sub(pattern.FirstSeen)
	if duration > 0 {
		pattern.Frequency = float64(pattern.Count) / duration.Hours()
	}
}

// GetFailurePatterns returns all failure patterns
func (t *FailureTracker) GetFailurePatterns() map[string]*FailurePattern {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	result := make(map[string]*FailurePattern)
	for k, v := range t.failures {
		result[k] = v
	}
	return result
}

// GetCriticalFailures returns critical failure patterns
func (t *FailureTracker) GetCriticalFailures() map[string]*FailurePattern {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	result := make(map[string]*FailurePattern)
	for k, v := range t.failures {
		if v.IsCritical {
			result[k] = v
		}
	}
	return result
}

// GetFrequentFailures returns failures with high frequency
func (t *FailureTracker) GetFrequentFailures(threshold float64) map[string]*FailurePattern {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	result := make(map[string]*FailurePattern)
	for k, v := range t.failures {
		if v.Frequency >= threshold {
			result[k] = v
		}
	}
	return result
}

// TimeoutTracker tracks timeout patterns
type TimeoutTracker struct {
	timeouts map[string]*TimeoutPattern
	mutex    sync.RWMutex
}

// TimeoutPattern represents a timeout pattern
type TimeoutPattern struct {
	ComponentType string
	AverageTime   time.Duration
	MaxTime       time.Duration
	MinTime       time.Duration
	Count         int64
	Threshold     time.Duration
}

// NewTimeoutTracker creates a new timeout tracker
func NewTimeoutTracker() *TimeoutTracker {
	return &TimeoutTracker{
		timeouts: make(map[string]*TimeoutPattern),
	}
}

// TrackTimeout tracks a timeout occurrence
func (t *TimeoutTracker) TrackTimeout(componentType string, duration, threshold time.Duration) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	pattern, exists := t.timeouts[componentType]
	if !exists {
		pattern = &TimeoutPattern{
			ComponentType: componentType,
			Count:         0,
			MinTime:       duration,
			MaxTime:       duration,
			AverageTime:   duration,
			Threshold:     threshold,
		}
		t.timeouts[componentType] = pattern
	}

	pattern.Count++

	if duration < pattern.MinTime {
		pattern.MinTime = duration
	}

	if duration > pattern.MaxTime {
		pattern.MaxTime = duration
	}

	// Calculate new average
	pattern.AverageTime = (pattern.AverageTime*time.Duration(pattern.Count-1) + duration) / time.Duration(pattern.Count)
}

// GetTimeoutPatterns returns all timeout patterns
func (t *TimeoutTracker) GetTimeoutPatterns() map[string]*TimeoutPattern {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	result := make(map[string]*TimeoutPattern)
	for k, v := range t.timeouts {
		result[k] = v
	}
	return result
}
