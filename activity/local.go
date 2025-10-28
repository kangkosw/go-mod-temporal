package activity

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"go.temporal.io/sdk/worker"
)

// LocalActivityOptions holds options for local activities
type LocalActivityOptions struct {
	ScheduleToCloseTimeout time.Duration
	StartToCloseTimeout    time.Duration
	RetryPolicy            *LocalRetryPolicy
}

// LocalRetryPolicy defines retry policy for local activities
type LocalRetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
}

// LocalRegistry manages local activity registration
type LocalRegistry struct {
	activities map[string]*LocalDefinition
}

// LocalDefinition holds local activity definition
type LocalDefinition struct {
	Name     string
	Function interface{}
	Options  *LocalActivityOptions
}

// NewLocalRegistry creates a new local activity registry
func NewLocalRegistry() *LocalRegistry {
	return &LocalRegistry{
		activities: make(map[string]*LocalDefinition),
	}
}

// Register registers a local activity
func (r *LocalRegistry) Register(name string, fn interface{}, options ...*LocalActivityOptions) error {
	if err := r.validateLocalActivity(fn); err != nil {
		return fmt.Errorf("invalid local activity function: %w", err)
	}

	var opts *LocalActivityOptions
	if len(options) > 0 {
		opts = options[0]
	}

	r.activities[name] = &LocalDefinition{
		Name:     name,
		Function: fn,
		Options:  opts,
	}

	return nil
}

// RegisterWithWorker registers all local activities with a worker
func (r *LocalRegistry) RegisterWithWorker(w worker.Worker) {
	for _, def := range r.activities {
		w.RegisterActivity(def.Function)
	}
}

// Get retrieves a local activity definition
func (r *LocalRegistry) Get(name string) (*LocalDefinition, bool) {
	def, exists := r.activities[name]
	return def, exists
}

// List returns all registered local activities
func (r *LocalRegistry) List() map[string]*LocalDefinition {
	result := make(map[string]*LocalDefinition)
	for name, def := range r.activities {
		result[name] = def
	}
	return result
}

// validateLocalActivity validates that the function is a valid local activity
func (r *LocalRegistry) validateLocalActivity(fn interface{}) error {
	if fn == nil {
		return fmt.Errorf("local activity function cannot be nil")
	}

	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("local activity must be a function")
	}

	// Local activities can have different signature requirements than regular activities
	// They can optionally have context.Context as first parameter
	if fnType.NumIn() > 0 {
		firstParam := fnType.In(0)
		contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
		if firstParam.Implements(contextType) {
			// If first param is context, that's good
		}
		// If not context, that's also fine for local activities
	}

	// Check return values
	if fnType.NumOut() < 1 || fnType.NumOut() > 2 {
		return fmt.Errorf("local activity function must return 1 or 2 values")
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

// LocalActivityExecutor provides utilities for executing local activities
type LocalActivityExecutor struct {
	registry *LocalRegistry
}

// NewLocalActivityExecutor creates a new local activity executor
func NewLocalActivityExecutor(registry *LocalRegistry) *LocalActivityExecutor {
	return &LocalActivityExecutor{
		registry: registry,
	}
}

// Execute executes a local activity by name
func (e *LocalActivityExecutor) Execute(ctx context.Context, name string, args ...interface{}) (interface{}, error) {
	def, exists := e.registry.Get(name)
	if !exists {
		return nil, fmt.Errorf("local activity not found: %s", name)
	}

	// Execute the local activity function directly
	fnValue := reflect.ValueOf(def.Function)
	fnType := reflect.TypeOf(def.Function)

	// Prepare arguments
	callArgs := make([]reflect.Value, 0)

	// Add context if function expects it
	if fnType.NumIn() > 0 {
		firstParam := fnType.In(0)
		contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
		if firstParam.Implements(contextType) {
			callArgs = append(callArgs, reflect.ValueOf(ctx))
		}
	}

	// Add other arguments
	for _, arg := range args {
		callArgs = append(callArgs, reflect.ValueOf(arg))
	}

	// Call the function
	results := fnValue.Call(callArgs)

	// Handle results
	if len(results) == 1 {
		return results[0].Interface(), nil
	} else if len(results) == 2 {
		result := results[0].Interface()
		err := results[1].Interface()
		if err != nil {
			return result, err.(error)
		}
		return result, nil
	}

	return nil, fmt.Errorf("unexpected number of return values")
}

// Helper functions for local activities

// RegisterSimpleLocalActivity registers a simple local activity
func RegisterSimpleLocalActivity(registry *LocalRegistry, name string, fn interface{}) error {
	return registry.Register(name, fn, &LocalActivityOptions{
		ScheduleToCloseTimeout: 10 * time.Second,
		StartToCloseTimeout:    10 * time.Second,
	})
}

// RegisterQuickLocalActivity registers a quick local activity with short timeouts
func RegisterQuickLocalActivity(registry *LocalRegistry, name string, fn interface{}) error {
	return registry.Register(name, fn, &LocalActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Second,
		StartToCloseTimeout:    5 * time.Second,
	})
}

// RegisterRetryableLocalActivity registers a local activity with retry policy
func RegisterRetryableLocalActivity(registry *LocalRegistry, name string, fn interface{}, retryPolicy *LocalRetryPolicy) error {
	return registry.Register(name, fn, &LocalActivityOptions{
		ScheduleToCloseTimeout: 30 * time.Second,
		StartToCloseTimeout:    30 * time.Second,
		RetryPolicy:            retryPolicy,
	})
}

// DefaultLocalRetryPolicy returns a default retry policy for local activities
func DefaultLocalRetryPolicy() *LocalRetryPolicy {
	return &LocalRetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2.0,
		MaximumInterval:    5 * time.Second,
		MaximumAttempts:    3,
	}
}

// NoRetryLocalPolicy returns a no-retry policy for local activities
func NoRetryLocalPolicy() *LocalRetryPolicy {
	return &LocalRetryPolicy{
		MaximumAttempts: 1,
	}
}

// AggressiveLocalRetryPolicy returns an aggressive retry policy for local activities
func AggressiveLocalRetryPolicy() *LocalRetryPolicy {
	return &LocalRetryPolicy{
		InitialInterval:    50 * time.Millisecond,
		BackoffCoefficient: 1.5,
		MaximumInterval:    2 * time.Second,
		MaximumAttempts:    10,
	}
}

// LocalActivityWrapper wraps a function to make it suitable for local activity
type LocalActivityWrapper struct {
	name string
	fn   interface{}
}

// NewLocalActivityWrapper creates a new local activity wrapper
func NewLocalActivityWrapper(name string, fn interface{}) *LocalActivityWrapper {
	return &LocalActivityWrapper{
		name: name,
		fn:   fn,
	}
}

// WithTimeout wraps the function with timeout handling
func (w *LocalActivityWrapper) WithTimeout(timeout time.Duration) interface{} {
	fnValue := reflect.ValueOf(w.fn)
	fnType := reflect.TypeOf(w.fn)

	return reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		ctx := context.Background()
		if len(args) > 0 {
			if ctxArg, ok := args[0].Interface().(context.Context); ok {
				ctx = ctxArg
			}
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Update context in args if present
		if len(args) > 0 {
			contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
			if args[0].Type().Implements(contextType) {
				args[0] = reflect.ValueOf(timeoutCtx)
			}
		}

		// Execute with timeout
		done := make(chan []reflect.Value, 1)
		go func() {
			done <- fnValue.Call(args)
		}()

		select {
		case results := <-done:
			return results
		case <-timeoutCtx.Done():
			// Return error if function has error return
			if fnType.NumOut() == 2 {
				errorType := reflect.TypeOf((*error)(nil)).Elem()
				if fnType.Out(1).Implements(errorType) {
					zeroResult := reflect.Zero(fnType.Out(0))
					errorResult := reflect.ValueOf(fmt.Errorf("local activity timeout after %v", timeout))
					return []reflect.Value{zeroResult, errorResult}
				}
			}
			panic(fmt.Sprintf("local activity timeout after %v", timeout))
		}
	}).Interface()
}
