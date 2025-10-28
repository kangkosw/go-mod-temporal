package utils

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TestSuite provides utilities for testing Temporal workflows and activities
type TestSuite struct {
	*testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
	t   *testing.T
}

// NewTestSuite creates a new test suite
func NewTestSuite() *TestSuite {
	ts := &TestSuite{
		WorkflowTestSuite: &testsuite.WorkflowTestSuite{},
	}
	ts.SetLogger(GetGlobalLogger())
	return ts
}

// SetTestingT sets the testing.T instance
func (ts *TestSuite) SetTestingT(t *testing.T) {
	ts.t = t
}

// SetupTest sets up a test environment
func (ts *TestSuite) SetupTest(t *testing.T) {
	ts.env = ts.NewTestWorkflowEnvironment()
	ts.env.SetTestTimeout(30 * time.Second)
	ts.env.SetWorkerOptions(worker.Options{
		EnableSessionWorker: true,
	})
}

// TearDownTest tears down the test environment
func (ts *TestSuite) TearDownTest() {
	if ts.env != nil && ts.t != nil {
		ts.env.AssertExpectations(ts.t)
	}
}

// WorkflowTest executes a workflow test
func (ts *TestSuite) WorkflowTest(workflowFn interface{}, args ...interface{}) *testsuite.TestWorkflowEnvironment {
	ts.env.ExecuteWorkflow(workflowFn, args...)
	return ts.env
}

// ActivityTest executes an activity test
func (ts *TestSuite) ActivityTest(activityFn interface{}, args ...interface{}) (interface{}, error) {
	env := ts.NewTestActivityEnvironment()
	env.SetTestTimeout(10 * time.Second)

	blob, err := env.ExecuteActivity(activityFn, args...)
	if err != nil {
		return nil, err
	}

	var result interface{}
	err = blob.Get(&result)
	return result, err
}

// MockWorkflow creates a mock workflow for testing
func (ts *TestSuite) MockWorkflow(workflowID string, result interface{}, err error) {
	ts.env.OnWorkflow(workflowID, mock.Anything).Return(result, err)
}

// MockActivity creates a mock activity for testing
func (ts *TestSuite) MockActivity(activityFn interface{}, result interface{}, err error) {
	ts.env.OnActivity(activityFn, mock.Anything).Return(result, err)
}

// MockTimer creates a mock timer for testing
func (ts *TestSuite) MockTimer(duration time.Duration) {
	ts.env.RegisterDelayedCallback(func() {
		// Timer callback
	}, duration)
}

// AssertWorkflowCompleted asserts that workflow completed successfully
func (ts *TestSuite) AssertWorkflowCompleted() {
	if !ts.env.IsWorkflowCompleted() {
		if ts.t != nil {
			ts.t.Error("Expected workflow to be completed")
		}
	}
}

// AssertWorkflowFailed asserts that workflow failed
func (ts *TestSuite) AssertWorkflowFailed() {
	if ts.env.IsWorkflowCompleted() && ts.env.GetWorkflowError() == nil {
		if ts.t != nil {
			ts.t.Error("Expected workflow to fail")
		}
	}
}

// AssertWorkflowCanceled asserts that workflow was canceled
func (ts *TestSuite) AssertWorkflowCanceled() {
	if !ts.env.IsWorkflowCompleted() || ts.env.GetWorkflowError() == nil {
		if ts.t != nil {
			ts.t.Error("Expected workflow to be canceled")
		}
	}
}

// TestHelper provides helper functions for testing
type TestHelper struct {
	suite *TestSuite
}

// NewTestHelper creates a new test helper
func NewTestHelper(suite *TestSuite) *TestHelper {
	return &TestHelper{suite: suite}
}

// CreateTestWorkflow creates a simple test workflow
func (h *TestHelper) CreateTestWorkflow(name string) interface{} {
	return func(ctx workflow.Context, input string) (string, error) {
		logger := workflow.GetLogger(ctx)
		logger.Info("Test workflow started", "input", input)

		// Simulate some work
		workflow.Sleep(ctx, 100*time.Millisecond)

		result := fmt.Sprintf("processed: %s", input)
		logger.Info("Test workflow completed", "result", result)
		return result, nil
	}
}

// CreateTestActivity creates a simple test activity
func (h *TestHelper) CreateTestActivity(name string) interface{} {
	return func(ctx context.Context, input string) (string, error) {
		// Simulate some work
		time.Sleep(50 * time.Millisecond)
		return fmt.Sprintf("activity result: %s", input), nil
	}
}

// CreateFailingWorkflow creates a workflow that always fails
func (h *TestHelper) CreateFailingWorkflow(errorMsg string) interface{} {
	return func(ctx workflow.Context, input string) (string, error) {
		return "", fmt.Errorf(errorMsg)
	}
}

// CreateFailingActivity creates an activity that always fails
func (h *TestHelper) CreateFailingActivity(errorMsg string) interface{} {
	return func(ctx context.Context, input string) (string, error) {
		return "", fmt.Errorf(errorMsg)
	}
}

// CreateRetryableActivity creates an activity that fails a few times then succeeds
func (h *TestHelper) CreateRetryableActivity(failTimes int) interface{} {
	attempts := 0
	return func(ctx context.Context, input string) (string, error) {
		attempts++
		if attempts <= failTimes {
			return "", fmt.Errorf("attempt %d failed", attempts)
		}
		return fmt.Sprintf("succeeded on attempt %d", attempts), nil
	}
}

// BenchmarkHelper provides utilities for benchmarking workflows
type BenchmarkHelper struct {
	metrics *MetricsCollector
}

// NewBenchmarkHelper creates a new benchmark helper
func NewBenchmarkHelper() *BenchmarkHelper {
	return &BenchmarkHelper{
		metrics: NewMetricsCollector("benchmark"),
	}
}

// BenchmarkWorkflow benchmarks workflow execution
func (b *BenchmarkHelper) BenchmarkWorkflow(
	tb testing.TB,
	workflowFn interface{},
	iterations int,
	args ...interface{},
) *BenchmarkResult {
	if benchmarkTB, ok := tb.(*testing.B); ok {
		benchmarkTB.ResetTimer()
	}

	result := &BenchmarkResult{
		Iterations: iterations,
		StartTime:  time.Now(),
	}

	for i := 0; i < iterations; i++ {
		start := time.Now()

		// Execute workflow (this would be replaced with actual execution)
		suite := NewTestSuite()
		suite.SetupTest(tb.(*testing.T))
		env := suite.WorkflowTest(workflowFn, args...)

		duration := time.Since(start)
		result.Durations = append(result.Durations, duration)

		if env.GetWorkflowError() != nil {
			result.Errors++
		} else {
			result.Successes++
		}
	}

	result.EndTime = time.Now()
	result.TotalDuration = result.EndTime.Sub(result.StartTime)
	result.calculateStats()

	return result
}

// BenchmarkResult holds benchmark results
type BenchmarkResult struct {
	Iterations    int
	Successes     int
	Errors        int
	StartTime     time.Time
	EndTime       time.Time
	TotalDuration time.Duration
	Durations     []time.Duration

	// Calculated stats
	MinDuration    time.Duration
	MaxDuration    time.Duration
	AvgDuration    time.Duration
	MedianDuration time.Duration
	P95Duration    time.Duration
	P99Duration    time.Duration
}

// calculateStats calculates benchmark statistics
func (r *BenchmarkResult) calculateStats() {
	if len(r.Durations) == 0 {
		return
	}

	// Sort durations for percentile calculations
	durations := make([]time.Duration, len(r.Durations))
	copy(durations, r.Durations)

	// Simple bubble sort for durations
	for i := 0; i < len(durations); i++ {
		for j := 0; j < len(durations)-1-i; j++ {
			if durations[j] > durations[j+1] {
				durations[j], durations[j+1] = durations[j+1], durations[j]
			}
		}
	}

	r.MinDuration = durations[0]
	r.MaxDuration = durations[len(durations)-1]

	// Calculate average
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	r.AvgDuration = total / time.Duration(len(durations))

	// Calculate percentiles
	r.MedianDuration = durations[len(durations)/2]
	r.P95Duration = durations[int(float64(len(durations))*0.95)]
	r.P99Duration = durations[int(float64(len(durations))*0.99)]
}

// String returns a string representation of benchmark results
func (r *BenchmarkResult) String() string {
	successRate := float64(r.Successes) / float64(r.Iterations) * 100

	return fmt.Sprintf(`Benchmark Results:
  Iterations: %d
  Successes: %d (%.2f%%)
  Errors: %d
  Total Duration: %v
  Average Duration: %v
  Min Duration: %v
  Max Duration: %v
  Median Duration: %v
  P95 Duration: %v
  P99 Duration: %v`,
		r.Iterations,
		r.Successes, successRate,
		r.Errors,
		r.TotalDuration,
		r.AvgDuration,
		r.MinDuration,
		r.MaxDuration,
		r.MedianDuration,
		r.P95Duration,
		r.P99Duration,
	)
}

// TestDataGenerator generates test data for workflows and activities
type TestDataGenerator struct{}

// NewTestDataGenerator creates a new test data generator
func NewTestDataGenerator() *TestDataGenerator {
	return &TestDataGenerator{}
}

// GenerateString generates a random string of specified length
func (g *TestDataGenerator) GenerateString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[i%len(charset)]
	}
	return string(result)
}

// GenerateWorkflowID generates a test workflow ID
func (g *TestDataGenerator) GenerateWorkflowID(prefix string) string {
	if prefix == "" {
		prefix = "test"
	}
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// GenerateTestInput generates test input data
func (g *TestDataGenerator) GenerateTestInput(inputType string) interface{} {
	switch inputType {
	case "string":
		return g.GenerateString(10)
	case "number":
		return time.Now().Unix() % 1000
	case "boolean":
		return time.Now().Unix()%2 == 0
	case "map":
		return map[string]interface{}{
			"key1": g.GenerateString(5),
			"key2": time.Now().Unix() % 100,
			"key3": true,
		}
	default:
		return g.GenerateString(10)
	}
}

// Utility functions for testing

// GetFunctionName returns the name of a function
func GetFunctionName(fn interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

// AssertNoError asserts that there is no error
func AssertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

// AssertError asserts that there is an error
func AssertError(t *testing.T, err error) {
	if err == nil {
		t.Error("Expected an error, got nil")
	}
}

// AssertEqual asserts that two values are equal
func AssertEqual(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

// AssertNotEqual asserts that two values are not equal
func AssertNotEqual(t *testing.T, expected, actual interface{}) {
	if reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected values to be different, but both are %v", expected)
	}
}
