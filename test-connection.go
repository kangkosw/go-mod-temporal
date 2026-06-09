package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	temporalclient "github.com/kangkosw/go-mod-temporal/client"
	"github.com/kangkosw/go-mod-temporal/worker"
	workflowpkg "github.com/kangkosw/go-mod-temporal/workflow"
)

// OrderWorkflow untuk demo
func OrderWorkflow(ctx workflow.Context, orderID string, customerName string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Processing order", "orderID", orderID, "customer", customerName)

	logger.Info("Validating order...")
	workflow.Sleep(ctx, 1*time.Second)

	logger.Info("Processing payment...")
	workflow.Sleep(ctx, 1*time.Second)

	logger.Info("Preparing shipment...")
	workflow.Sleep(ctx, 1*time.Second)

	result := fmt.Sprintf("Order %s for %s processed successfully at %s",
		orderID, customerName, workflow.Now(ctx).Format("15:04:05"))

	logger.Info("Order processing completed", "result", result)
	return result, nil
}

func main() {
	log.Println("🚀 Testing Real Connection to Temporal Docker...")

	// Connect to existing Temporal server
	temporalClient, err := temporalclient.NewClientWithConfig(&temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	})
	if err != nil {
		log.Fatal("❌ Failed to create client:", err)
	}
	defer temporalClient.Close()

	log.Println("✅ Connected to Temporal server at localhost:7233")

	// Test health check
	if err := temporalClient.HealthCheck(context.Background()); err != nil {
		log.Printf("⚠️ Health check failed: %v", err)
	} else {
		log.Println("✅ Health check passed")
	}

	// Setup worker - use the underlying client
	workerManager := worker.NewManager(temporalClient.Client)
	orderWorker, err := workerManager.AddWorker("test-orders", nil)
	if err != nil {
		log.Fatal("❌ Failed to create worker:", err)
	}

	orderWorker.RegisterWorkflow(OrderWorkflow)

	if err := orderWorker.Start(); err != nil {
		log.Fatal("❌ Failed to start worker:", err)
	}
	defer orderWorker.Stop()

	log.Println("✅ Worker started successfully")

	// Generate unique ID
	idGenerator := workflowpkg.NewIDGenerator(&workflowpkg.IDConfig{
		Strategy: workflowpkg.UUIDStrategy,
	})

	ctx := context.Background()

	// Execute workflow
	log.Println("\n=== Executing Test Order ===")

	workflowID := fmt.Sprintf("test-order-%s", idGenerator.Generate())
	log.Printf("Workflow ID: %s", workflowID)

	workflowRun, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "test-orders",
	}, OrderWorkflow, "ORD-12345", "John Doe")

	if err != nil {
		log.Printf("❌ Failed to start workflow: %v", err)
		return
	}

	log.Println("⏳ Workflow executing...")

	var result string
	err = workflowRun.Get(ctx, &result)
	if err != nil {
		log.Printf("❌ Workflow failed: %v", err)
	} else {
		log.Printf("✅ Workflow completed: %s", result)
	}

	log.Printf("\n🌐 View in Temporal Web UI: http://localhost:8080/namespaces/default/workflows/%s", workflowID)
	log.Println("\n🎊 Test completed successfully!")
}
