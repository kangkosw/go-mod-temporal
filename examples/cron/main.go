package main

import (
	"context"
	"log"
	"time"

	"github.com/hantulautt/go-mod-temporal/client"
	"github.com/hantulautt/go-mod-temporal/patterns"
	"github.com/hantulautt/go-mod-temporal/worker"
	"go.temporal.io/sdk/workflow"
)

// ReportWorkflow is a sample workflow that generates reports
func ReportWorkflow(ctx workflow.Context, reportType string) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Report workflow started", "reportType", reportType)

	// Simulate report generation
	workflow.Sleep(ctx, 5*time.Second)

	result := "Report generated successfully: " + reportType + " at " + workflow.Now(ctx).Format(time.RFC3339)
	logger.Info("Report workflow completed", "result", result)

	return result, nil
}

func main() {
	// 1. Setup client
	config := client.DefaultConfig()
	config.HostPort = "localhost:7233"
	config.Namespace = "default"

	temporalClient, err := client.NewClientWithConfig(config)
	if err != nil {
		log.Fatal("Failed to create client:", err)
	}
	defer temporalClient.Close()

	// 2. Setup worker
	workerManager := worker.NewManager(temporalClient.Client)
	w, err := workerManager.AddWorker("cron-reports", nil)
	if err != nil {
		log.Fatal("Failed to create worker:", err)
	}

	// Register workflow
	w.RegisterWorkflow(ReportWorkflow)

	// Start worker
	if err := w.Start(); err != nil {
		log.Fatal("Failed to start worker:", err)
	}
	defer w.Stop()

	log.Println("Worker started for cron jobs...")

	// 3. Create different types of cron jobs

	// Daily report at 9:00 AM
	dailyReport := patterns.DailyCronJob(
		temporalClient,
		ReportWorkflow,
		"cron-reports",
		9, 0, // 9:00 AM
		"daily-sales-report",
	)

	if err := dailyReport.Start(context.Background()); err != nil {
		log.Printf("Failed to start daily report cron: %v", err)
	} else {
		log.Printf("Daily report cron started: %s", dailyReport.GetID())
	}

	// Hourly report at minute 30
	hourlyReport := patterns.HourlyCronJob(
		temporalClient,
		ReportWorkflow,
		"cron-reports",
		30, // minute 30 of every hour
		"hourly-status-report",
	)

	if err := hourlyReport.Start(context.Background()); err != nil {
		log.Printf("Failed to start hourly report cron: %v", err)
	} else {
		log.Printf("Hourly report cron started: %s", hourlyReport.GetID())
	}

	// Weekly report every Monday at 8:00 AM
	weeklyReport := patterns.WeeklyCronJob(
		temporalClient,
		ReportWorkflow,
		"cron-reports",
		1, 8, 0, // Monday at 8:00 AM
		"weekly-summary-report",
	)

	if err := weeklyReport.Start(context.Background()); err != nil {
		log.Printf("Failed to start weekly report cron: %v", err)
	} else {
		log.Printf("Weekly report cron started: %s", weeklyReport.GetID())
	}

	// Custom cron job - every 5 minutes for testing
	testReport := patterns.CustomCronJob(
		temporalClient,
		"*/5 * * * *", // Every 5 minutes
		ReportWorkflow,
		"cron-reports",
		"test-report",
	)

	if err := testReport.Start(context.Background()); err != nil {
		log.Printf("Failed to start test report cron: %v", err)
	} else {
		log.Printf("Test report cron started: %s", testReport.GetID())
	}

	// Resilient cron job that continues despite failures
	resilientReport := patterns.ResilientCronJob(
		temporalClient,
		"0 */6 * * *", // Every 6 hours
		ReportWorkflow,
		"cron-reports",
		"resilient-report",
	)

	if err := resilientReport.Start(context.Background()); err != nil {
		log.Printf("Failed to start resilient report cron: %v", err)
	} else {
		log.Printf("Resilient report cron started: %s", resilientReport.GetID())
	}

	// 4. Monitor and manage cron jobs
	log.Println("\nCron jobs created. You can:")
	log.Println("- Check status of daily report:", dailyReport.GetID())
	log.Println("- Pause/resume jobs using the cron job methods")
	log.Println("- Trigger jobs manually for testing")

	// Example: Trigger a job manually
	log.Println("\nTriggering daily report manually for demonstration...")
	if err := dailyReport.Trigger(context.Background()); err != nil {
		log.Printf("Failed to trigger daily report: %v", err)
	} else {
		log.Println("Daily report triggered successfully!")
	}

	// Example: Get job status
	status, err := dailyReport.GetStatus(context.Background())
	if err != nil {
		log.Printf("Failed to get status: %v", err)
	} else {
		log.Printf("Daily report status: %+v", status)
	}

	// Keep the program running
	log.Println("\nPress Ctrl+C to stop...")
	select {}
}

/*
To run this example:

1. Start Temporal server (using Docker):
   docker run --rm -p 7233:7233 -p 8233:8233 temporalio/auto-setup:latest

2. Run the worker:
   go run examples/cron/main.go

3. The cron jobs will be created and scheduled according to their expressions:
   - Daily report: runs at 9:00 AM every day
   - Hourly report: runs at 30 minutes past every hour
   - Weekly report: runs every Monday at 8:00 AM
   - Test report: runs every 5 minutes (for testing)
   - Resilient report: runs every 6 hours, continues on failures

4. You can monitor the executions in Temporal Web UI at http://localhost:8233

Features demonstrated:
✅ Cron job dengan berbagai pola (daily, hourly, weekly, custom)
✅ WorkflowID generation otomatis
✅ Retry policy untuk failure handling
✅ Manual trigger untuk testing
✅ Status monitoring
✅ Resilient execution yang continue meski ada failure
✅ Worker management dan registration
*/
