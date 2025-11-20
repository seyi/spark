// Spark AI Agents CLI
// Command-line interface for managing distributed AI agents
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/apache/spark/spark-ai-agents/pkg/agent"
	"github.com/apache/spark/spark-ai-agents/pkg/coordinator"
	"github.com/apache/spark/spark-ai-agents/pkg/evaluation"
	"github.com/apache/spark/spark-ai-agents/pkg/scheduler"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "eval":
		handleEval()
	case "status":
		handleStatus()
	case "list-models":
		handleListModels()
	case "list-agents":
		handleListAgents()
	case "metrics":
		handleMetrics()
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	usage := `
Spark AI Agents CLI

Usage:
  spark-agents <command> [options]

Commands:
  eval <file>          Run evaluation from .evalset.json file
  status <job-id>      Check status of a job
  list-models          List available models
  list-agents          List registered agents
  metrics              Show coordinator metrics
  help                 Show this help message

Examples:
  spark-agents eval tests/my-eval.evalset.json
  spark-agents status job-12345
  spark-agents list-models
  spark-agents metrics

For more information, visit: https://github.com/apache/spark/spark-ai-agents
`
	fmt.Println(usage)
}

func handleEval() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: spark-agents eval <evalset-file>")
		os.Exit(1)
	}

	filepath := os.Args[2]

	fmt.Printf("Loading evaluation set from: %s\n", filepath)

	evalSet, err := evaluation.LoadEvaluationSet(filepath)
	if err != nil {
		fmt.Printf("Error loading evaluation set: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Evaluation: %s\n", evalSet.Name)
	fmt.Printf("Test cases: %d\n", len(evalSet.TestCases))

	// Create coordinator
	coord, err := coordinator.NewSparkAgentCoordinator(nil)
	if err != nil {
		fmt.Printf("Error creating coordinator: %s\n", err)
		os.Exit(1)
	}
	defer coord.Shutdown()

	// Create evaluator
	evaluator := evaluation.NewEvaluator(&CoordinatorAdapter{coord: coord})

	// Run evaluation
	fmt.Println("\nRunning evaluation...")
	report, err := evaluator.RunEvaluation(context.Background(), evalSet)
	if err != nil {
		fmt.Printf("Error running evaluation: %s\n", err)
		os.Exit(1)
	}

	// Print report
	evaluation.PrintReport(report)

	// Save report
	reportPath := filepath + ".report.json"
	reportData, _ := json.MarshalIndent(report, "", "  ")
	os.WriteFile(reportPath, reportData, 0644)
	fmt.Printf("Report saved to: %s\n", reportPath)
}

func handleStatus() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: spark-agents status <job-id>")
		os.Exit(1)
	}

	jobID := os.Args[2]

	coord, err := coordinator.NewSparkAgentCoordinator(nil)
	if err != nil {
		fmt.Printf("Error creating coordinator: %s\n", err)
		os.Exit(1)
	}
	defer coord.Shutdown()

	job, err := coord.GetJobStatus(jobID)
	if err != nil {
		fmt.Printf("Error getting job status: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nJob Status: %s\n", jobID)
	fmt.Printf("State: %v\n", job.State)
	fmt.Printf("Stages: %d total, %d completed\n", len(job.DAG.Stages), len(job.Results))
	fmt.Printf("Start time: %s\n", job.StartTime)
	if job.State == 2 || job.State == 3 { // Completed or Failed
		fmt.Printf("End time: %s\n", job.EndTime)
		fmt.Printf("Duration: %s\n", job.EndTime.Sub(job.StartTime))
	}
	if job.Error != nil {
		fmt.Printf("Error: %s\n", job.Error)
	}
}

func handleListModels() {
	coord, err := coordinator.NewSparkAgentCoordinator(nil)
	if err != nil {
		fmt.Printf("Error creating coordinator: %s\n", err)
		os.Exit(1)
	}
	defer coord.Shutdown()

	models := coord.GetModelRegistry().List()

	fmt.Println("\nAvailable Models:")
	for i, model := range models {
		fmt.Printf("  %d. %s\n", i+1, model)
	}
	fmt.Printf("\nTotal: %d models\n", len(models))
}

func handleListAgents() {
	fmt.Println("\nRegistered Agents:")
	fmt.Println("(Agent registration requires running coordinator)")
}

func handleMetrics() {
	coord, err := coordinator.NewSparkAgentCoordinator(nil)
	if err != nil {
		fmt.Printf("Error creating coordinator: %s\n", err)
		os.Exit(1)
	}
	defer coord.Shutdown()

	metrics := coord.GetMetrics()

	fmt.Println("\nCoordinator Metrics:")
	fmt.Printf("  Registered Agents: %d\n", metrics.RegisteredAgents)
	fmt.Printf("  Registered Models: %d\n", metrics.RegisteredModels)
	fmt.Printf("  Total Executors: %d\n", metrics.TotalExecutors)
	fmt.Printf("  Active Tasks: %d\n", metrics.ActiveTasks)
	fmt.Printf("  Queued Tasks: %d\n", metrics.QueuedTasks)
	fmt.Printf("  Registered Tools: %d\n", metrics.RegisteredTools)
	fmt.Printf("  Active Sessions: %d\n", metrics.ActiveSessions)
	fmt.Printf("  Stored Memories: %d\n", metrics.StoredMemories)
}

// CoordinatorAdapter adapts SparkAgentCoordinator to evaluation.AgentCoordinator
type CoordinatorAdapter struct {
	coord *coordinator.SparkAgentCoordinator
}

func (a *CoordinatorAdapter) SubmitJob(ctx context.Context, agentID string, input *agent.AgentInput) (string, error) {
	return a.coord.SubmitJob(ctx, agentID, input)
}

func (a *CoordinatorAdapter) GetJobStatus(jobID string) (evaluation.JobStatus, error) {
	job, err := a.coord.GetJobStatus(jobID)
	if err != nil {
		return nil, err
	}
	return &JobStatusAdapter{job: job}, nil
}

// JobStatusAdapter adapts scheduler.Job to evaluation.JobStatus
type JobStatusAdapter struct {
	job *scheduler.Job
}

func (j *JobStatusAdapter) IsComplete() bool {
	return j.job.State == 2 || j.job.State == 3 // JobCompleted or JobFailed
}

func (j *JobStatusAdapter) GetOutput() *agent.AgentOutput {
	// Get output from last completed stage
	if len(j.job.Results) == 0 {
		return nil
	}

	// Find result stage (stage 0)
	if result, ok := j.job.Results[0]; ok && len(result.Outputs) > 0 {
		for _, output := range result.Outputs {
			return output
		}
	}

	return nil
}

func (j *JobStatusAdapter) GetError() error {
	return j.job.Error
}
