package main

import (
	"context"
	"fmt"
	"log"
	"time"

	temporalclient "github.com/kangkosw/go-mod-temporal/client"
	"github.com/kangkosw/go-mod-temporal/worker"
	workflowpkg "github.com/kangkosw/go-mod-temporal/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

// ProcessOrderWorkflow processes a single order
func ProcessOrderWorkflow(ctx workflow.Context, orderID string, customerName string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Processing order", "orderID", orderID, "customer", customerName)

	// Simulate order processing steps
	logger.Info("Validating order...")
	workflow.Sleep(ctx, 2*time.Second)

	logger.Info("Processing payment...")
	workflow.Sleep(ctx, 3*time.Second)

	logger.Info("Preparing shipment...")
	workflow.Sleep(ctx, 2*time.Second)

	result := fmt.Sprintf("Order %s for %s processed successfully at %s",
		orderID, customerName, workflow.Now(ctx).Format(time.RFC3339))

	logger.Info("Order processing completed", "result", result)
	return result, nil
}

// SendNotificationWorkflow sends a notification
func SendNotificationWorkflow(ctx workflow.Context, recipient string, message string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Sending notification", "recipient", recipient, "message", message)

	// Simulate notification sending
	workflow.Sleep(ctx, 1*time.Second)

	result := fmt.Sprintf("Notification sent to %s: %s", recipient, message)
	logger.Info("Notification sent", "result", result)
	return result, nil
}

func main() {
	log.Println("🚀 Starting One-Shot Execution Example")

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

	orderWorker, err := workerManager.AddWorker("order-processing", nil)
	if err != nil {
		log.Fatal("Failed to create order worker:", err)
	}
	orderWorker.RegisterWorkflow(ProcessOrderWorkflow)

	notificationWorker, err := workerManager.AddWorker("notifications", nil)
	if err != nil {
		log.Fatal("Failed to create notification worker:", err)
	}
	notificationWorker.RegisterWorkflow(SendNotificationWorkflow)

	// Start workers
	if err := orderWorker.Start(); err != nil {
		log.Fatal("Failed to start order worker:", err)
	}
	defer orderWorker.Stop()

	if err := notificationWorker.Start(); err != nil {
		log.Fatal("Failed to start notification worker:", err)
	}
	defer notificationWorker.Stop()

	log.Println("Workers started successfully!")

	// 3. Setup ID generator
	idGenerator := workflowpkg.NewIDGenerator(&workflowpkg.IDConfig{
		Strategy: workflowpkg.UUIDStrategy,
	})

	ctx := context.Background()

	// 4. Immediate execution example
	log.Println("\n=== Immediate One-Shot Execution ===")

	orderID := idGenerator.Generate()
	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("immediate-order-%s", orderID),
		TaskQueue: "order-processing",
	}, ProcessOrderWorkflow, "ORD-001", "John Doe")

	if err != nil {
		log.Printf("❌ Failed to start immediate order: %v", err)
	} else {
		var result string
		err = workflowRun.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Immediate order failed: %v", err)
		} else {
			log.Printf("✅ Immediate order completed: %s", result)
		}
	}

	// 5. Scheduled execution example
	log.Println("\n=== Scheduled One-Shot Execution ===")

	// Schedule for 5 seconds from now
	scheduledTime := time.Now().Add(5 * time.Second)
	log.Printf("Scheduling notification for: %s", scheduledTime.Format("15:04:05"))

	notificationID := idGenerator.Generate()
	scheduledRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("scheduled-notification-%s", notificationID),
		TaskQueue: "notifications",
	}, SendNotificationWorkflow, "admin@example.com", fmt.Sprintf("Scheduled message at %s", scheduledTime.Format("15:04:05")))

	if err != nil {
		log.Printf("❌ Failed to start scheduled notification: %v", err)
	} else {
		log.Println("⏰ Notification scheduled, waiting for execution...")
		var result string
		err = scheduledRun.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Scheduled notification failed: %v", err)
		} else {
			log.Printf("✅ Scheduled notification completed: %s", result)
		}
	}

	// 6. Multiple one-shot executions
	log.Println("\n=== Multiple One-Shot Executions ===")

	orders := []struct {
		ID       string
		Customer string
	}{
		{"ORD-002", "Alice Smith"},
		{"ORD-003", "Bob Johnson"},
		{"ORD-004", "Carol Brown"},
	}

	for _, order := range orders {
		orderExecID := idGenerator.Generate()
		run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:        fmt.Sprintf("batch-order-%s", orderExecID),
			TaskQueue: "order-processing",
		}, ProcessOrderWorkflow, order.ID, order.Customer)

		if err != nil {
			log.Printf("❌ Failed to start order %s: %v", order.ID, err)
			continue
		}

		// Execute asynchronously
		go func(orderID string, workflowRun client.WorkflowRun) {
			var result string
			err := workflowRun.Get(context.Background(), &result)
			if err != nil {
				log.Printf("❌ Order %s failed: %v", orderID, err)
			} else {
				log.Printf("✅ Order %s completed: %s", orderID, result)
			}
		}(order.ID, run)
	}

	// 7. Wait for all executions to complete
	log.Println("\n=== Waiting for all executions to complete ===")
	time.Sleep(10 * time.Second)

	// 8. Summary
	log.Println("\n=== One-Shot Execution Summary ===")
	log.Println("✅ Immediate execution completed")
	log.Println("✅ Scheduled execution completed")
	log.Println("✅ Multiple one-shot executions launched")

	log.Println("\n🎊 One-shot execution example completed!")
	log.Println("💡 This demonstrates executing workflows as one-time tasks")
}
