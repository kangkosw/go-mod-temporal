package activity

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

// Definition holds activity definition
type Definition struct {
	Name     string
	Function interface{}
	Options  *Options
}

// Options holds activity options
type Options struct {
	Name                   string
	TaskQueue              string
	ScheduleToCloseTimeout string
	ScheduleToStartTimeout string
	StartToCloseTimeout    string
	HeartbeatTimeout       string
	WaitForCancellation    bool
	ActivityID             string
	RetryPolicy            *RetryPolicy
	DisableEagerExecution  bool
}

// RetryPolicy defines retry policy for activities
type RetryPolicy struct {
	InitialInterval        string
	BackoffCoefficient     float64
	MaximumInterval        string
	MaximumAttempts        int32
	NonRetryableErrorTypes []string
}

// Registry manages activity registration
type Registry struct {
	activities map[string]*Definition
	mutex      sync.RWMutex
}

// NewRegistry creates a new activity registry
func NewRegistry() *Registry {
	return &Registry{
		activities: make(map[string]*Definition),
	}
}

// Register registers an activity function
func (r *Registry) Register(name string, fn interface{}, options ...*Options) error {
	if err := r.validateActivity(fn); err != nil {
		return fmt.Errorf("invalid activity function: %w", err)
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	var opts *Options
	if len(options) > 0 {
		opts = options[0]
	}

	if opts == nil {
		opts = &Options{}
	}

	if opts.Name == "" {
		opts.Name = name
	}

	r.activities[name] = &Definition{
		Name:     name,
		Function: fn,
		Options:  opts,
	}

	return nil
}

// RegisterWithOptions registers an activity with specific options
func (r *Registry) RegisterWithOptions(fn interface{}, options *Options) error {
	if options == nil || options.Name == "" {
		return fmt.Errorf("activity name is required")
	}

	return r.Register(options.Name, fn, options)
}

// Get retrieves an activity definition
func (r *Registry) Get(name string) (*Definition, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	def, exists := r.activities[name]
	return def, exists
}

// List returns all registered activities
func (r *Registry) List() map[string]*Definition {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make(map[string]*Definition)
	for name, def := range r.activities {
		result[name] = def
	}
	return result
}

// RegisterWithWorker registers all activities with a worker
func (r *Registry) RegisterWithWorker(w worker.Worker) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for _, def := range r.activities {
		if def.Options != nil && def.Options.Name != "" {
			w.RegisterActivityWithOptions(def.Function, activity.RegisterOptions{
				Name: def.Options.Name,
			})
		} else {
			w.RegisterActivity(def.Function)
		}
	}
}

// validateActivity validates that the function is a valid activity
func (r *Registry) validateActivity(fn interface{}) error {
	if fn == nil {
		return fmt.Errorf("activity function cannot be nil")
	}

	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("activity must be a function")
	}

	// Check if first parameter is context.Context
	if fnType.NumIn() < 1 {
		return fmt.Errorf("activity function must have at least one parameter (context.Context)")
	}

	firstParam := fnType.In(0)
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !firstParam.Implements(contextType) {
		return fmt.Errorf("first parameter must be context.Context")
	}

	// Check return values
	if fnType.NumOut() < 1 || fnType.NumOut() > 2 {
		return fmt.Errorf("activity function must return 1 or 2 values")
	}

	// If 2 return values, second must be error
	if fnType.NumOut() == 2 {
		errorType := reflect.TypeOf((*error)(nil)).Elem()
		if !fnType.Out(1).Implements(errorType) {
			return fmt.Errorf("second return value must be error")
		}
	}

	return nil
}

// Helper functions for common activity patterns

// RegisterSimpleActivity registers a simple activity with default options
func RegisterSimpleActivity(registry *Registry, name string, fn interface{}) error {
	return registry.Register(name, fn, &Options{
		Name: name,
	})
}

// RegisterRetryableActivity registers an activity with retry policy
func RegisterRetryableActivity(registry *Registry, name string, fn interface{}, retryPolicy *RetryPolicy) error {
	return registry.Register(name, fn, &Options{
		Name:        name,
		RetryPolicy: retryPolicy,
	})
}

// RegisterLocalActivity registers a local activity
func RegisterLocalActivity(registry *Registry, name string, fn interface{}) error {
	return registry.Register(name, fn, &Options{
		Name: name,
		// Local activities are handled differently in registration
	})
}

// DefaultRetryPolicy returns a default retry policy
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		InitialInterval:    "1s",
		BackoffCoefficient: 2.0,
		MaximumInterval:    "30s",
		MaximumAttempts:    3,
	}
}

// AggressiveRetryPolicy returns an aggressive retry policy
func AggressiveRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		InitialInterval:    "100ms",
		BackoffCoefficient: 1.5,
		MaximumInterval:    "10s",
		MaximumAttempts:    10,
	}
}

// NoRetryPolicy returns a no-retry policy
func NoRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaximumAttempts: 1,
	}
}
