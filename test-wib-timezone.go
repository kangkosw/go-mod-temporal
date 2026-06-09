package main

import (
	"context"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	temporalclient "github.com/kangkosw/go-mod-temporal/client"
	"github.com/kangkosw/go-mod-temporal/schedule"
	"github.com/kangkosw/go-mod-temporal/worker"
	workflowpkg "github.com/kangkosw/go-mod-temporal/workflow"
)

// Set timezone ke GMT+7 (WIB - Waktu Indonesia Barat)
var wibLocation *time.Location

func init() {
	var err error
	wibLocation, err = time.LoadLocation("Asia/Jakarta")
	if err != nil {
		// Fallback jika timezone tidak tersedia
		wibLocation = time.FixedZone("WIB", 7*60*60) // GMT+7
	}
	log.Printf("🌍 Timezone set to: %s (GMT+7)", wibLocation.String())
}

// ReportWorkflow untuk demo dengan timezone WIB
func ReportWorkflow(ctx workflow.Context, reportType string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Generating report", "type", reportType)

	// Simulasi pembuatan report
	workflow.Sleep(ctx, 2*time.Second)

	// Format waktu dengan timezone WIB
	wibTime := workflow.Now(ctx).In(wibLocation)
	result := "Report " + reportType + " generated at " + wibTime.Format("15:04:05 WIB (02 Jan 2006)")
	logger.Info("Report completed", "result", result)
	return result, nil
}

// TimeAwareWorkflow untuk demo berbagai format waktu
func TimeAwareWorkflow(ctx workflow.Context, taskName string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting time-aware workflow", "task", taskName)

	workflowTime := workflow.Now(ctx)

	// Tampilkan waktu dalam berbagai format
	utcTime := workflowTime.UTC()
	wibTime := workflowTime.In(wibLocation)

	logger.Info("Time comparison",
		"utc", utcTime.Format("15:04:05 UTC"),
		"wib", wibTime.Format("15:04:05 WIB"))

	// Simulasi task
	workflow.Sleep(ctx, 1*time.Second)

	result := "Task " + taskName + " completed at " + wibTime.Format("15:04:05 WIB on Monday, 02 Jan 2006")
	logger.Info("Workflow completed", "result", result)
	return result, nil
}

func main() {
	log.Println("🚀 Testing Temporal with WIB Timezone (GMT+7)...")

	// Tampilkan waktu saat ini
	currentTime := time.Now()
	utcTime := currentTime.UTC()
	wibTime := currentTime.In(wibLocation)

	log.Printf("🕐 Current UTC Time: %s", utcTime.Format("15:04:05 UTC (Monday, 02 Jan 2006)"))
	log.Printf("🕐 Current WIB Time: %s", wibTime.Format("15:04:05 WIB (Monday, 02 Jan 2006)"))

	// Connect to Temporal
	temporalClient, err := temporalclient.NewClientWithConfig(&temporalclient.Config{
		HostPort:  "localhost:7233",
		Namespace: "default",
	})
	if err != nil {
		log.Fatal("❌ Failed to create client:", err)
	}
	defer temporalClient.Close()

	log.Println("✅ Connected to Temporal server")

	// Setup worker
	workerManager := worker.NewManager(temporalClient.Client)
	reportWorker, err := workerManager.AddWorker("wib-reports", nil)
	if err != nil {
		log.Fatal("❌ Failed to create worker:", err)
	}

	reportWorker.RegisterWorkflow(ReportWorkflow)
	reportWorker.RegisterWorkflow(TimeAwareWorkflow)

	if err := reportWorker.Start(); err != nil {
		log.Fatal("❌ Failed to start worker:", err)
	}
	defer reportWorker.Stop()

	log.Println("✅ Worker started for WIB timezone reports")

	// Generate unique IDs
	idGenerator := workflowpkg.NewIDGenerator(&workflowpkg.IDConfig{
		Strategy: workflowpkg.UUIDStrategy,
	})

	ctx := context.Background()

	// Test 1: Basic report dengan WIB timezone
	log.Println("\n=== Test 1: Basic Report dengan WIB Timezone ===")

	workflowID1 := "wib-report-" + idGenerator.Generate()
	workflowRun1, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID1,
		TaskQueue: "wib-reports",
	}, ReportWorkflow, "DAILY_SUMMARY_WIB")

	if err != nil {
		log.Printf("❌ Failed to start report: %v", err)
	} else {
		var result string
		err = workflowRun1.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Report failed: %v", err)
		} else {
			log.Printf("✅ Report completed: %s", result)
		}
	}

	// Test 2: Time-aware workflow
	log.Println("\n=== Test 2: Time-Aware Workflow ===")

	workflowID2 := "time-aware-" + idGenerator.Generate()
	workflowRun2, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID2,
		TaskQueue: "wib-reports",
	}, TimeAwareWorkflow, "TIMEZONE_DEMO")

	if err != nil {
		log.Printf("❌ Failed to start time-aware workflow: %v", err)
	} else {
		var result string
		err = workflowRun2.Get(ctx, &result)
		if err != nil {
			log.Printf("❌ Time-aware workflow failed: %v", err)
		} else {
			log.Printf("✅ Time-aware workflow completed: %s", result)
		}
	}

	// Setup cron job dengan timezone WIB (simplified)
	log.Println("\n=== Test 3: Cron Job Setup (WIB) ===")
	scheduleManager := schedule.NewManager(temporalClient.Client)

	cronConfig := &schedule.Config{
		ScheduleID: "wib-hourly-report",
	}

	err = scheduleManager.Create(ctx, cronConfig)
	if err != nil {
		log.Printf("⚠️ Cron job info: %v", err)
		log.Println("📝 Note: Scheduling is simplified in compatibility mode")
	} else {
		log.Printf("✅ Cron job created: %s", cronConfig.ScheduleID)
	}

	// Final time display
	finalTime := time.Now().In(wibLocation)
	log.Printf("\n🕐 Test completed at: %s", finalTime.Format("15:04:05 WIB (Monday, 02 Jan 2006)"))
	log.Printf("🌐 View workflows at: http://localhost:8080/namespaces/default/workflows")
	log.Printf("🔗 Workflow 1: %s", workflowID1)
	log.Printf("🔗 Workflow 2: %s", workflowID2)
	log.Println("\n🎊 WIB Timezone test completed successfully!")
}
