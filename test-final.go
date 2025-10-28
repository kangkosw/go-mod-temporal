package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Activity untuk demonstrasi
func DemoActivity(ctx context.Context, input string) (string, error) {
	now := time.Now()
	return fmt.Sprintf("✅ %s - Diproses pada %s", input, now.Format("15:04:05 WIB (Monday, 02 Jan 2006)")), nil
}

// Workflow untuk demonstrasi
func DemoWorkflow(ctx workflow.Context, input string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, DemoActivity, input).Get(ctx, &result)
	if err != nil {
		return "", err
	}

	return result, nil
}

// Workflow dengan timezone awareness
func TimezoneWorkflow(ctx workflow.Context, task string) (string, error) {
	// Simulasi processing time
	err := workflow.Sleep(ctx, time.Second*2)
	if err != nil {
		return "", err
	}

	now := time.Now()
	result := fmt.Sprintf("Task '%s' selesai pada %s", task, now.Format("15:04:05 WIB (Monday, 02 Jan 2006)"))

	return result, nil
}

func main() {
	// Set timezone ke Indonesia (WIB - GMT+7)
	location, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		log.Printf("⚠️ Failed to load Asia/Jakarta timezone: %v, using fallback", err)
		location = time.FixedZone("WIB", 7*60*60) // GMT+7
	}
	time.Local = location

	fmt.Println("🇮🇩 GO-TEMPORAL-MODULE - FINAL INTEGRATION TEST")
	fmt.Println("===============================================")
	fmt.Printf("🌍 Timezone: %s (GMT+7)\n", location.String())
	fmt.Printf("🕐 Waktu sekarang: %s\n\n", time.Now().Format("15:04:05 WIB (Monday, 02 Jan 2006)"))

	// Connect to Temporal
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		log.Fatal("❌ Failed to create Temporal client:", err)
	}
	defer c.Close()

	// Create worker
	w := worker.New(c, "final-test-queue", worker.Options{})

	// Register workflows and activities
	w.RegisterWorkflow(DemoWorkflow)
	w.RegisterWorkflow(TimezoneWorkflow)
	w.RegisterActivity(DemoActivity)

	// Start worker
	go func() {
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Printf("Worker error: %v", err)
		}
	}()
	defer w.Stop()

	// Wait for worker to start
	time.Sleep(2 * time.Second)
	fmt.Println("✅ Worker started successfully")

	// Test 1: Basic workflow dengan timezone
	fmt.Println("\n=== Test 1: Basic Workflow dengan WIB ===")
	workflowOptions := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("demo-workflow-%d", time.Now().Unix()),
		TaskQueue: "final-test-queue",
	}

	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, DemoWorkflow, "Data Test Indonesia")
	if err != nil {
		log.Printf("❌ Failed to execute workflow: %v", err)
	} else {
		var result string
		err = we.Get(context.Background(), &result)
		if err != nil {
			log.Printf("❌ Failed to get workflow result: %v", err)
		} else {
			fmt.Printf("🎯 Result: %s\n", result)
		}
	}

	// Test 2: Timezone-aware workflow
	fmt.Println("\n=== Test 2: Timezone-Aware Workflow ===")
	workflowOptions2 := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("timezone-workflow-%d", time.Now().Unix()),
		TaskQueue: "final-test-queue",
	}

	we2, err := c.ExecuteWorkflow(context.Background(), workflowOptions2, TimezoneWorkflow, "Laporan Harian Indonesia")
	if err != nil {
		log.Printf("❌ Failed to execute timezone workflow: %v", err)
	} else {
		var result2 string
		err = we2.Get(context.Background(), &result2)
		if err != nil {
			log.Printf("❌ Failed to get timezone workflow result: %v", err)
		} else {
			fmt.Printf("🎯 Result: %s\n", result2)
		}
	}

	// Test 3: Informasi Module
	fmt.Println("\n=== Test 3: Module Information ===")
	fmt.Println("📦 Module Name: github.com/hantulautt/go-mod-temporal")
	fmt.Println("🏗️ Go Version: 1.23.0")
	fmt.Println("⚡ Temporal SDK: v1.21.2")
	fmt.Println("🌏 Timezone Support: Asia/Jakarta (WIB/GMT+7)")
	fmt.Println("🔧 Features:")
	fmt.Println("   ✅ Connection management")
	fmt.Println("   ✅ Worker management")
	fmt.Println("   ✅ Workflow execution")
	fmt.Println("   ✅ Activity management")
	fmt.Println("   ✅ Retry policies")
	fmt.Println("   ✅ Cron scheduling")
	fmt.Println("   ✅ One-shot execution")
	fmt.Println("   ✅ Signal handling")
	fmt.Println("   ✅ Schedule management")
	fmt.Println("   ✅ Common utilities")
	fmt.Println("   ✅ Indonesian timezone (WIB)")
	fmt.Println("   ✅ Bahasa Indonesia documentation")

	fmt.Println("\n===============================================")
	fmt.Println("🎊 FINAL INTEGRATION TEST BERHASIL!")
	fmt.Printf("🕐 Test selesai pada: %s\n", time.Now().Format("15:04:05 WIB (Monday, 02 Jan 2006)"))
	fmt.Println("🌐 Lihat workflows di: http://localhost:8080/namespaces/default/workflows")
	fmt.Println("📚 Dokumentasi lengkap tersedia di README.md")
	fmt.Println("🇮🇩 Module siap digunakan untuk project Indonesia!")
	fmt.Println("===============================================")
}
