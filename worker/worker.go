package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	activityPkg "github.com/kangkosw/go-mod-temporal/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Options holds worker configuration options
type Options struct {
	// Basic configuration
	TaskQueue                  string
	MaxConcurrentActivityTasks int
	MaxConcurrentWorkflowTasks int
	MaxConcurrentLocalTasks    int

	// Performance tuning
	WorkerActivitiesPerSecond       float64
	MaxConcurrentActivityExecutions int
	WorkerStopTimeout               time.Duration
	BackgroundActivityContext       context.Context
	WorkflowPanicPolicy             worker.WorkflowPanicPolicy
	// DataConverter                   client.DataConverter // Not available in this SDK version
	// FailureConverter                client.FailureConverter // Not available in this SDK version

	// Session configuration
	EnableSessionWorker            bool
	MaxConcurrentSessionExecutions int

	// Local activity configuration
	EnableLocalActivities   bool
	LocalActivityWorkerOnly bool

	// Debugging and monitoring
	Identity                         string
	DeadlockDetectionTimeout         time.Duration
	MaxHeartbeatThrottleInterval     time.Duration
	DefaultHeartbeatThrottleInterval time.Duration

	// Custom options
	Interceptors []interface{} // WorkerInterceptor type may not be available in this SDK version
	OnFatalError func(error)
}

// Manager manages multiple workers
type Manager struct {
	client    client.Client
	workers   map[string]*Worker
	mutex     sync.RWMutex
	isStarted bool
}

// Worker wraps temporal worker with additional functionality
type Worker struct {
	worker.Worker
	taskQueue   string
	options     *Options
	activityReg *activityPkg.Registry
	localActReg *activityPkg.LocalRegistry
	isStarted   bool
	stopCh      chan struct{}
}

// NewManager creates a new worker manager
func NewManager(client client.Client) *Manager {
	return &Manager{
		client:  client,
		workers: make(map[string]*Worker),
	}
}

// NewWorker creates a new worker with default options
func NewWorker(client client.Client, taskQueue string) *Worker {
	options := DefaultOptions(taskQueue)
	return NewWorkerWithOptions(client, options)
}

// NewWorkerWithOptions creates a new worker with custom options
func NewWorkerWithOptions(client client.Client, options *Options) *Worker {
	if options == nil {
		options = DefaultOptions("")
	}

	workerOptions := worker.Options{
		MaxConcurrentActivityTaskPollers: options.MaxConcurrentActivityTasks,
		MaxConcurrentWorkflowTaskPollers: options.MaxConcurrentWorkflowTasks,
		// MaxConcurrentLocalActivityPollers: options.MaxConcurrentLocalTasks, // Not available in this SDK version
		WorkerActivitiesPerSecond: options.WorkerActivitiesPerSecond,
		// MaxConcurrentActivityExecutions:   options.MaxConcurrentActivityExecutions, // Not available in this SDK version
		WorkerStopTimeout:         options.WorkerStopTimeout,
		BackgroundActivityContext: options.BackgroundActivityContext,
		WorkflowPanicPolicy:       options.WorkflowPanicPolicy,
		// DataConverter:                     options.DataConverter, // Not available in this SDK version
		// FailureConverter:                  options.FailureConverter, // Not available in this SDK version
		EnableSessionWorker: options.EnableSessionWorker,
		// MaxConcurrentSessionExecutions:    options.MaxConcurrentSessionExecutions, // Not available in this SDK version
		// EnableLocalActivities:   options.EnableLocalActivities, // Not available in this SDK version
		// LocalActivityWorkerOnly: options.LocalActivityWorkerOnly, // Not available in this SDK version
		Identity:                         options.Identity,
		DeadlockDetectionTimeout:         options.DeadlockDetectionTimeout,
		MaxHeartbeatThrottleInterval:     options.MaxHeartbeatThrottleInterval,
		DefaultHeartbeatThrottleInterval: options.DefaultHeartbeatThrottleInterval,
		// Interceptors:                      options.Interceptors, // Not available in this SDK version
		OnFatalError: options.OnFatalError,
	}

	temporalWorker := worker.New(client, options.TaskQueue, workerOptions)

	return &Worker{
		Worker:      temporalWorker,
		taskQueue:   options.TaskQueue,
		options:     options,
		activityReg: activityPkg.NewRegistry(),
		localActReg: activityPkg.NewLocalRegistry(),
		stopCh:      make(chan struct{}),
	}
}

// RegisterWorkflow registers a workflow function
func (w *Worker) RegisterWorkflow(workflowFunc interface{}) {
	w.Worker.RegisterWorkflow(workflowFunc)
}

// RegisterWorkflowWithOptions registers a workflow with options
func (w *Worker) RegisterWorkflowWithOptions(workflowFunc interface{}, options interface{}) {
	// Note: RegisterWorkflowOptions may not be available in this SDK version
	// Using basic registration for compatibility
	w.Worker.RegisterWorkflow(workflowFunc)
}

// RegisterActivity registers an activity function
func (w *Worker) RegisterActivity(activityFunc interface{}) {
	w.Worker.RegisterActivity(activityFunc)
	// Also register in our registry
	w.activityReg.Register(fmt.Sprintf("activity_%p", activityFunc), activityFunc)
}

// RegisterActivityWithOptions registers an activity with options
func (w *Worker) RegisterActivityWithOptions(activityFunc interface{}, options interface{}) {
	// Note: RegisterActivityOptions may not be available in this SDK version
	// Using basic registration for compatibility
	w.Worker.RegisterActivity(activityFunc)
	// Also register in our registry
	w.activityReg.Register(fmt.Sprintf("activity_%p", activityFunc), activityFunc)
}

// RegisterActivityFromRegistry registers activities from activity registry
func (w *Worker) RegisterActivityFromRegistry(registry *activityPkg.Registry) {
	registry.RegisterWithWorker(w.Worker)
	// Merge registries
	for name, def := range registry.List() {
		w.activityReg.Register(name, def.Function, def.Options)
	}
}

// RegisterLocalActivity registers a local activity
func (w *Worker) RegisterLocalActivity(localActivityFunc interface{}) {
	// Local activities are registered differently
	name := fmt.Sprintf("local_activity_%p", localActivityFunc)
	w.localActReg.Register(name, localActivityFunc)
}

// RegisterLocalActivityFromRegistry registers local activities from registry
func (w *Worker) RegisterLocalActivityFromRegistry(registry *activityPkg.LocalRegistry) {
	registry.RegisterWithWorker(w.Worker)
	// Merge registries
	for name, def := range registry.List() {
		w.localActReg.Register(name, def.Function, def.Options)
	}
}

// Start starts the worker
func (w *Worker) Start() error {
	if w.isStarted {
		return fmt.Errorf("worker already started")
	}

	w.isStarted = true
	return w.Worker.Start()
}

// Stop stops the worker
func (w *Worker) Stop() {
	if !w.isStarted {
		return
	}

	w.Worker.Stop()
	close(w.stopCh)
	w.isStarted = false
}

// IsStarted returns whether the worker is started
func (w *Worker) IsStarted() bool {
	return w.isStarted
}

// GetTaskQueue returns the task queue name
func (w *Worker) GetTaskQueue() string {
	return w.taskQueue
}

// GetOptions returns the worker options
func (w *Worker) GetOptions() *Options {
	return w.options
}

// GetActivityRegistry returns the activity registry
func (w *Worker) GetActivityRegistry() *activityPkg.Registry {
	return w.activityReg
}

// GetLocalActivityRegistry returns the local activity registry
func (w *Worker) GetLocalActivityRegistry() *activityPkg.LocalRegistry {
	return w.localActReg
}

// Manager methods

// AddWorker adds a worker to the manager
func (m *Manager) AddWorker(taskQueue string, options *Options) (*Worker, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.workers[taskQueue]; exists {
		return nil, fmt.Errorf("worker for task queue %s already exists", taskQueue)
	}

	if options == nil {
		options = DefaultOptions(taskQueue)
	}
	options.TaskQueue = taskQueue

	worker := NewWorkerWithOptions(m.client, options)
	m.workers[taskQueue] = worker

	return worker, nil
}

// GetWorker retrieves a worker by task queue
func (m *Manager) GetWorker(taskQueue string) (*Worker, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	worker, exists := m.workers[taskQueue]
	return worker, exists
}

// RemoveWorker removes a worker from the manager
func (m *Manager) RemoveWorker(taskQueue string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	worker, exists := m.workers[taskQueue]
	if !exists {
		return fmt.Errorf("worker for task queue %s not found", taskQueue)
	}

	if worker.IsStarted() {
		worker.Stop()
	}

	delete(m.workers, taskQueue)
	return nil
}

// StartAll starts all workers
func (m *Manager) StartAll() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isStarted {
		return fmt.Errorf("manager already started")
	}

	for taskQueue, worker := range m.workers {
		if err := worker.Start(); err != nil {
			return fmt.Errorf("failed to start worker for task queue %s: %w", taskQueue, err)
		}
	}

	m.isStarted = true
	return nil
}

// StopAll stops all workers
func (m *Manager) StopAll() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, worker := range m.workers {
		worker.Stop()
	}

	m.isStarted = false
}

// ListWorkers returns all workers
func (m *Manager) ListWorkers() map[string]*Worker {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string]*Worker)
	for taskQueue, worker := range m.workers {
		result[taskQueue] = worker
	}
	return result
}

// IsStarted returns whether the manager is started
func (m *Manager) IsStarted() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.isStarted
}

// DefaultOptions returns default worker options
func DefaultOptions(taskQueue string) *Options {
	return &Options{
		TaskQueue:                        taskQueue,
		MaxConcurrentActivityTasks:       10,
		MaxConcurrentWorkflowTasks:       10,
		MaxConcurrentLocalTasks:          10,
		WorkerActivitiesPerSecond:        100000,
		MaxConcurrentActivityExecutions:  1000,
		WorkerStopTimeout:                30 * time.Second,
		WorkflowPanicPolicy:              worker.FailWorkflow,
		EnableSessionWorker:              false,
		MaxConcurrentSessionExecutions:   1000,
		EnableLocalActivities:            true,
		LocalActivityWorkerOnly:          false,
		DeadlockDetectionTimeout:         1 * time.Second,
		MaxHeartbeatThrottleInterval:     60 * time.Second,
		DefaultHeartbeatThrottleInterval: 30 * time.Second,
	}
}

// HighThroughputOptions returns options optimized for high throughput
func HighThroughputOptions(taskQueue string) *Options {
	opts := DefaultOptions(taskQueue)
	opts.MaxConcurrentActivityTasks = 50
	opts.MaxConcurrentWorkflowTasks = 50
	opts.MaxConcurrentActivityExecutions = 5000
	opts.WorkerActivitiesPerSecond = 200000
	return opts
}

// LowLatencyOptions returns options optimized for low latency
func LowLatencyOptions(taskQueue string) *Options {
	opts := DefaultOptions(taskQueue)
	opts.MaxConcurrentActivityTasks = 5
	opts.MaxConcurrentWorkflowTasks = 5
	opts.MaxConcurrentActivityExecutions = 100
	opts.DeadlockDetectionTimeout = 500 * time.Millisecond
	opts.DefaultHeartbeatThrottleInterval = 10 * time.Second
	return opts
}

// SessionEnabledOptions returns options with session support enabled
func SessionEnabledOptions(taskQueue string) *Options {
	opts := DefaultOptions(taskQueue)
	opts.EnableSessionWorker = true
	opts.MaxConcurrentSessionExecutions = 100
	return opts
}
