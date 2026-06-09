package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	temporalclient "github.com/kangkosw/go-mod-temporal/client"
	"github.com/kangkosw/go-mod-temporal/worker"
	workflowpkg "github.com/kangkosw/go-mod-temporal/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// UnreliableProcessWorkflow simulates a process that may fail
func UnreliableProcessWorkflow(ctx workflow.Context, taskID string, failureRate float64) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting unreliable process", "taskID", taskID, "failureRate", failureRate)

	// Simulate random failures
	if rand.Float64() < failureRate {
		err := fmt.Errorf("process failed for task %s (simulated failure)", taskID)
		logger.Error("Process failed", "error", err)
		return "", err
	}

	// Simulate processing time
	workflow.Sleep(ctx, 2*time.Second)

	result := fmt.Sprintf("Task %s completed successfully at %s", taskID, workflow.Now(ctx).Format("15:04:05"))
	logger.Info("Process completed", "result", result)
	return result, nil
}

// NetworkCallWorkflow simulates network calls that may timeout
func NetworkCallWorkflow(ctx workflow.Context, endpoint string, timeout time.Duration) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Making network call", "endpoint", endpoint, "timeout", timeout)

	// Simulate network delay
	delay := time.Duration(rand.Intn(6)) * time.Second
	if delay > timeout {
		err := fmt.Errorf("network call to %s timed out after %v", endpoint, delay)
		logger.Error("Network call timed out", "error", err)
		return "", err
	}

	workflow.Sleep(ctx, delay)

	result := fmt.Sprintf("Network call to %s completed in %v", endpoint, delay)
	logger.Info("Network call completed", "result", result)
	return result, nil
}

// DatabaseWorkflow simulates database operations that may fail
func DatabaseWorkflow(ctx workflow.Context, operation string, recordID string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Database operation", "operation", operation, "recordID", recordID)

	// Simulate database connection issues (20% failure rate)
	if rand.Float64() < 0.2 {
		err := fmt.Errorf("database connection failed for %s operation on record %s", operation, recordID)
		logger.Error("Database operation failed", "error", err)
		return "", err
	}

	// Simulate operation time
	workflow.Sleep(ctx, 1*time.Second)

	result := fmt.Sprintf("Database %s operation on record %s completed", operation, recordID)
	logger.Info("Database operation completed", "result", result)
	return result, nil
}

func main() {
	log.Println("🚀 Starting Retry Pattern Example")
	rand.Seed(time.Now().UnixNano())

	// 1. Setup client
	temporalClient, err := temporalclient.NewClientWithConfig(&temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	})
	if err != nil {
		log.Fatal("Failed to create Temporal client:", err)
	}
	defer temporalClient.Close()

	// Health check
	if err := temporalClient.HealthCheck(context.Background()); err != nil {
		log.Printf("⚠️  Health check failed (continuing anyway): %v", err)
	} else {
		log.Println("✅ Temporal client health check passed")
	}

	// 2. Setup workers
	workerManager := worker.NewManager(temporalClient)

	processWorker, err := workerManager.AddWorker("unreliable-processing", nil)
	if err != nil {
		log.Fatal("Failed to create process worker:", err)
	}
	processWorker.RegisterWorkflow(UnreliableProcessWorkflow)

	networkWorker, err := workerManager.AddWorker("network-calls", nil)
	if err != nil {
		log.Fatal("Failed to create network worker:", err)
	}
	networkWorker.RegisterWorkflow(NetworkCallWorkflow)

	dbWorker, err := workerManager.AddWorker("database-operations", nil)
	if err != nil {
		log.Fatal("Failed to create database worker:", err)
	}
	dbWorker.RegisterWorkflow(DatabaseWorkflow)

	// Start workers
	if err := processWorker.Start(); err != nil {
		log.Fatal("Failed to start process worker:", err)
	}
	defer processWorker.Stop()

	if err := networkWorker.Start(); err != nil {
		log.Fatal("Failed to start network worker:", err)
	}
	defer networkWorker.Stop()

	if err := dbWorker.Start(); err != nil {
		log.Fatal("Failed to start database worker:", err)
	}
	defer dbWorker.Stop()

	log.Println("Workers started successfully!")

	// 3. Setup ID generator
	idGenerator := workflowpkg.NewIDGenerator(&workflowpkg.IDConfig{
		Strategy: workflowpkg.UUIDStrategy,
	})

	ctx := context.Background()

	// 4. Basic retry pattern with Temporal's built-in retry
	log.Println("\n=== Basic Retry Pattern ===")

	taskID := idGenerator.Generate()
	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("basic-retry-%s", taskID),
		TaskQueue: "unreliable-processing",
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    time.Second,
			MaximumInterval:    10 * time.Second,
			BackoffCoefficient: 2.0,
		},
	}, UnreliableProcessWorkflow, "TASK-001", 0.6) // 60% failure rate

	if err != nil {
		log.Printf("❌ Failed to start basic retry: %v", err)
	} else {
		var result string
		err = workflowRun.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Basic retry finally failed: %v", err)
		} else {
			log.Printf("✅ Basic retry succeeded: %s", result)
		}
	}

	// 5. Network timeout retry pattern
	log.Println("\n=== Network Timeout Retry Pattern ===")

	networkID := idGenerator.Generate()
	networkRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("network-retry-%s", networkID),
		TaskQueue: "network-calls",
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    5,
			InitialInterval:    500 * time.Millisecond,
			MaximumInterval:    5 * time.Second,
			BackoffCoefficient: 1.5,
		},
	}, NetworkCallWorkflow, "https://api.example.com/data", 3*time.Second)

	if err != nil {
		log.Printf("❌ Failed to start network retry: %v", err)
	} else {
		var result string
		err = networkRun.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Network retry finally failed: %v", err)
		} else {
			log.Printf("✅ Network retry succeeded: %s", result)
		}
	}

	// 6. Database operation retry pattern
	log.Println("\n=== Database Operation Retry Pattern ===")

	dbID := idGenerator.Generate()
	dbRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("database-retry-%s", dbID),
		TaskQueue: "database-operations",
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    4,
			InitialInterval:    2 * time.Second,
			MaximumInterval:    30 * time.Second,
			BackoffCoefficient: 3.0,
		},
	}, DatabaseWorkflow, "UPDATE", "USER-123")

	if err != nil {
		log.Printf("❌ Failed to start database retry: %v", err)
	} else {
		var result string
		err = dbRun.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Database retry finally failed: %v", err)
		} else {
			log.Printf("✅ Database retry succeeded: %s", result)
		}
	}

	// 7. Multiple retry executions
	log.Println("\n=== Multiple Retry Executions ===")

	tasks := []struct {
		ID          string
		FailureRate float64
		Description string
	}{
		{"BATCH-001", 0.3, "Low failure rate task"},
		{"BATCH-002", 0.7, "High failure rate task"},
		{"BATCH-003", 0.1, "Very reliable task"},
	}

	for _, task := range tasks {
		batchID := idGenerator.Generate()
		run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        fmt.Sprintf("batch-retry-%s", batchID),
			TaskQueue: "unreliable-processing",
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts:    5,
				InitialInterval:    time.Second,
				MaximumInterval:    20 * time.Second,
				BackoffCoefficient: 2.0,
			},
		}, UnreliableProcessWorkflow, task.ID, task.FailureRate)

		if err != nil {
			log.Printf("❌ Failed to start batch task %s: %v", task.ID, err)
			continue
		}

		// Execute asynchronously
		go func(taskInfo struct {
			ID          string
			FailureRate float64
			Description string
		}, workflowRun client.WorkflowRun) {
			var result string
			err := workflowRun.Get(context.Background(), &result)
			if err != nil {
				log.Printf("❌ Batch task %s (%s) finally failed: %v", taskInfo.ID, taskInfo.Description, err)
			} else {
				log.Printf("✅ Batch task %s (%s) succeeded: %s", taskInfo.ID, taskInfo.Description, result)
			}
		}(task, run)
	}

	// 8. Wait for all executions to complete
	log.Println("\n=== Waiting for all executions to complete ===")
	time.Sleep(15 * time.Second)

	// 9. Summary
	log.Println("\n=== Retry Pattern Summary ===")
	log.Println("✅ Basic retry pattern executed")
	log.Println("✅ Network timeout retry pattern executed")
	log.Println("✅ Database operation retry pattern executed")
	log.Println("✅ Multiple retry executions launched")

	log.Println("\n🎊 Retry pattern example completed!")
	log.Println("💡 This demonstrates using Temporal's built-in retry policies")
	log.Println("💡 Retry policies provide exponential backoff and maximum attempt limits")
}
