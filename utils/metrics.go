package utils

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MetricsCollector collects and exports Temporal metrics
type MetricsCollector struct {
	// Workflow metrics
	workflowStarted   prometheus.Counter
	workflowCompleted *prometheus.CounterVec
	workflowDuration  *prometheus.HistogramVec
	workflowsActive   prometheus.Gauge

	// Activity metrics
	activityStarted   prometheus.Counter
	activityCompleted *prometheus.CounterVec
	activityDuration  *prometheus.HistogramVec
	activitiesActive  prometheus.Gauge
	activityRetries   *prometheus.CounterVec

	// Schedule metrics
	scheduleExecutions *prometheus.CounterVec
	scheduleLag        *prometheus.HistogramVec

	// Worker metrics
	workersActive prometheus.Gauge
	taskQueueLag  *prometheus.GaugeVec

	// Custom metrics
	customCounters   map[string]prometheus.Counter
	customGauges     map[string]prometheus.Gauge
	customHistograms map[string]prometheus.Histogram

	mutex sync.RWMutex
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(namespace string) *MetricsCollector {
	if namespace == "" {
		namespace = "temporal"
	}

	return &MetricsCollector{
		// Workflow metrics
		workflowStarted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "workflows_started_total",
			Help:      "Total number of workflows started",
		}),
		workflowCompleted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "workflows_completed_total",
			Help:      "Total number of workflows completed",
		}, []string{"status", "workflow_type"}),
		workflowDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "workflow_duration_seconds",
			Help:      "Workflow execution duration in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{"workflow_type", "status"}),
		workflowsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "workflows_active",
			Help:      "Number of currently active workflows",
		}),

		// Activity metrics
		activityStarted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "activities_started_total",
			Help:      "Total number of activities started",
		}),
		activityCompleted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "activities_completed_total",
			Help:      "Total number of activities completed",
		}, []string{"status", "activity_type"}),
		activityDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "activity_duration_seconds",
			Help:      "Activity execution duration in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{"activity_type", "status"}),
		activitiesActive: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "activities_active",
			Help:      "Number of currently active activities",
		}),
		activityRetries: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "activity_retries_total",
			Help:      "Total number of activity retries",
		}, []string{"activity_type"}),

		// Schedule metrics
		scheduleExecutions: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "schedule_executions_total",
			Help:      "Total number of schedule executions",
		}, []string{"schedule_id", "status"}),
		scheduleLag: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "schedule_lag_seconds",
			Help:      "Schedule execution lag in seconds",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300},
		}, []string{"schedule_id"}),

		// Worker metrics
		workersActive: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "workers_active",
			Help:      "Number of active workers",
		}),
		taskQueueLag: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "task_queue_lag",
			Help:      "Task queue lag",
		}, []string{"task_queue", "task_type"}),

		// Custom metrics
		customCounters:   make(map[string]prometheus.Counter),
		customGauges:     make(map[string]prometheus.Gauge),
		customHistograms: make(map[string]prometheus.Histogram),
	}
}

// Workflow metrics methods

// RecordWorkflowStarted records a workflow start
func (m *MetricsCollector) RecordWorkflowStarted() {
	m.workflowStarted.Inc()
	m.workflowsActive.Inc()
}

// RecordWorkflowCompleted records a workflow completion
func (m *MetricsCollector) RecordWorkflowCompleted(workflowType, status string, duration time.Duration) {
	m.workflowCompleted.WithLabelValues(status, workflowType).Inc()
	m.workflowDuration.WithLabelValues(workflowType, status).Observe(duration.Seconds())
	m.workflowsActive.Dec()
}

// Activity metrics methods

// RecordActivityStarted records an activity start
func (m *MetricsCollector) RecordActivityStarted() {
	m.activityStarted.Inc()
	m.activitiesActive.Inc()
}

// RecordActivityCompleted records an activity completion
func (m *MetricsCollector) RecordActivityCompleted(activityType, status string, duration time.Duration) {
	m.activityCompleted.WithLabelValues(status, activityType).Inc()
	m.activityDuration.WithLabelValues(activityType, status).Observe(duration.Seconds())
	m.activitiesActive.Dec()
}

// RecordActivityRetry records an activity retry
func (m *MetricsCollector) RecordActivityRetry(activityType string) {
	m.activityRetries.WithLabelValues(activityType).Inc()
}

// Schedule metrics methods

// RecordScheduleExecution records a schedule execution
func (m *MetricsCollector) RecordScheduleExecution(scheduleID, status string, lag time.Duration) {
	m.scheduleExecutions.WithLabelValues(scheduleID, status).Inc()
	if lag > 0 {
		m.scheduleLag.WithLabelValues(scheduleID).Observe(lag.Seconds())
	}
}

// Worker metrics methods

// SetWorkersActive sets the number of active workers
func (m *MetricsCollector) SetWorkersActive(count int) {
	m.workersActive.Set(float64(count))
}

// SetTaskQueueLag sets the task queue lag
func (m *MetricsCollector) SetTaskQueueLag(taskQueue, taskType string, lag float64) {
	m.taskQueueLag.WithLabelValues(taskQueue, taskType).Set(lag)
}

// Custom metrics methods

// RegisterCounter registers a custom counter metric
func (m *MetricsCollector) RegisterCounter(name, help string) prometheus.Counter {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if counter, exists := m.customCounters[name]; exists {
		return counter
	}

	counter := promauto.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})

	m.customCounters[name] = counter
	return counter
}

// RegisterGauge registers a custom gauge metric
func (m *MetricsCollector) RegisterGauge(name, help string) prometheus.Gauge {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if gauge, exists := m.customGauges[name]; exists {
		return gauge
	}

	gauge := promauto.NewGauge(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	})

	m.customGauges[name] = gauge
	return gauge
}

// RegisterHistogram registers a custom histogram metric
func (m *MetricsCollector) RegisterHistogram(name, help string, buckets []float64) prometheus.Histogram {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if histogram, exists := m.customHistograms[name]; exists {
		return histogram
	}

	if buckets == nil {
		buckets = prometheus.DefBuckets
	}

	histogram := promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	})

	m.customHistograms[name] = histogram
	return histogram
}

// GetCounter retrieves a custom counter
func (m *MetricsCollector) GetCounter(name string) (prometheus.Counter, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	counter, exists := m.customCounters[name]
	return counter, exists
}

// GetGauge retrieves a custom gauge
func (m *MetricsCollector) GetGauge(name string) (prometheus.Gauge, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	gauge, exists := m.customGauges[name]
	return gauge, exists
}

// GetHistogram retrieves a custom histogram
func (m *MetricsCollector) GetHistogram(name string) (prometheus.Histogram, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	histogram, exists := m.customHistograms[name]
	return histogram, exists
}

// MetricsMiddleware provides metrics collection middleware
type MetricsMiddleware struct {
	collector *MetricsCollector
	logger    *Logger
}

// NewMetricsMiddleware creates a new metrics middleware
func NewMetricsMiddleware(collector *MetricsCollector, logger *Logger) *MetricsMiddleware {
	return &MetricsMiddleware{
		collector: collector,
		logger:    logger,
	}
}

// TrackWorkflow tracks workflow execution
func (m *MetricsMiddleware) TrackWorkflow(workflowType string, fn func() error) error {
	start := time.Now()
	m.collector.RecordWorkflowStarted()

	err := fn()
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "failure"
	}

	m.collector.RecordWorkflowCompleted(workflowType, status, duration)

	if m.logger != nil {
		m.logger.Info("Workflow execution tracked",
			"workflowType", workflowType,
			"status", status,
			"duration", duration,
		)
	}

	return err
}

// TrackActivity tracks activity execution
func (m *MetricsMiddleware) TrackActivity(activityType string, fn func() error) error {
	start := time.Now()
	m.collector.RecordActivityStarted()

	err := fn()
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "failure"
	}

	m.collector.RecordActivityCompleted(activityType, status, duration)

	if m.logger != nil {
		m.logger.Info("Activity execution tracked",
			"activityType", activityType,
			"status", status,
			"duration", duration,
		)
	}

	return err
}

// PerformanceMonitor monitors system performance
type PerformanceMonitor struct {
	collector *MetricsCollector
	logger    *Logger
	interval  time.Duration
	stopCh    chan struct{}
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(collector *MetricsCollector, logger *Logger, interval time.Duration) *PerformanceMonitor {
	if interval == 0 {
		interval = 30 * time.Second
	}

	return &PerformanceMonitor{
		collector: collector,
		logger:    logger,
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
}

// Start starts the performance monitor
func (p *PerformanceMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.collectSystemMetrics()
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop stops the performance monitor
func (p *PerformanceMonitor) Stop() {
	close(p.stopCh)
}

// collectSystemMetrics collects system performance metrics
func (p *PerformanceMonitor) collectSystemMetrics() {
	// This would typically collect system metrics like:
	// - Memory usage
	// - CPU usage
	// - Goroutine count
	// - GC statistics
	// For now, we'll create placeholder metrics

	if memGauge, exists := p.collector.GetGauge("system_memory_usage"); exists {
		// Placeholder: would get actual memory usage
		memGauge.Set(0.75) // 75% memory usage
	}

	if cpuGauge, exists := p.collector.GetGauge("system_cpu_usage"); exists {
		// Placeholder: would get actual CPU usage
		cpuGauge.Set(0.45) // 45% CPU usage
	}

	if p.logger != nil {
		p.logger.Debug("System metrics collected")
	}
}

// AlertManager manages metric-based alerts
type AlertManager struct {
	collector *MetricsCollector
	logger    *Logger
	rules     []AlertRule
	mutex     sync.RWMutex
}

// AlertRule defines an alerting rule
type AlertRule struct {
	Name       string
	MetricName string
	Threshold  float64
	Condition  string // "gt", "lt", "eq"
	Duration   time.Duration
	Action     func(rule AlertRule, value float64)
}

// NewAlertManager creates a new alert manager
func NewAlertManager(collector *MetricsCollector, logger *Logger) *AlertManager {
	return &AlertManager{
		collector: collector,
		logger:    logger,
		rules:     make([]AlertRule, 0),
	}
}

// AddRule adds an alerting rule
func (a *AlertManager) AddRule(rule AlertRule) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.rules = append(a.rules, rule)
}

// Start starts the alert manager
func (a *AlertManager) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.checkRules()
		case <-ctx.Done():
			return
		}
	}
}

// checkRules checks all alerting rules
func (a *AlertManager) checkRules() {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	for _, rule := range a.rules {
		// This would check the actual metric values
		// For now, it's a placeholder
		if a.logger != nil {
			a.logger.Debug("Checking alert rule", "rule", rule.Name)
		}
	}
}

// Global metrics collector
var globalMetricsCollector *MetricsCollector

// InitGlobalMetrics initializes the global metrics collector
func InitGlobalMetrics(namespace string) {
	globalMetricsCollector = NewMetricsCollector(namespace)
}

// GetGlobalMetrics returns the global metrics collector
func GetGlobalMetrics() *MetricsCollector {
	if globalMetricsCollector == nil {
		globalMetricsCollector = NewMetricsCollector("temporal")
	}
	return globalMetricsCollector
}
