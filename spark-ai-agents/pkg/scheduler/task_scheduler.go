// Package scheduler implements task-level scheduling
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seyi/dagens/pkg/agent"
	"github.com/seyi/dagens/pkg/executor"
)

// TaskScheduler is responsible for low-level task scheduling and assignment.
// Inspired by Spark's TaskScheduler with locality-aware scheduling.
type TaskScheduler interface {
	SubmitTask(ctx context.Context, jobID string, task *agent.AgentTask) error
	GetTaskStatus(taskID string) (*agent.AgentTask, error)
	Stop() error
}

// LocalityLevel defines the preference for task placement
type LocalityLevel int

const (
	ProcessLocal LocalityLevel = iota // Same process/partition
	NodeLocal                         // Same node
	RackLocal                         // Same rack/region
	Any                              // Any location
)

func (l LocalityLevel) String() string {
	return [...]string{"PROCESS_LOCAL", "NODE_LOCAL", "RACK_LOCAL", "ANY"}[l]
}

// TaskSchedulerImpl implements TaskScheduler with locality-aware scheduling
type TaskSchedulerImpl struct {
	executorManager executor.ExecutorManager
	taskQueue       chan *TaskSubmission
	activeTasks     map[string]*agent.AgentTask
	taskResults     chan *TaskResult
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	dagScheduler    *DAGScheduler // Reference back for event notification

	// Scheduling configuration
	maxLocalityWaitTime time.Duration
	schedulingMode      SchedulingMode
}

// SchedulingMode determines how tasks are scheduled
type SchedulingMode int

const (
	FIFO SchedulingMode = iota // First in, first out
	Fair                       // Fair sharing between jobs
)

// TaskSubmission represents a task submitted for scheduling
type TaskSubmission struct {
	JobID    string
	Task     *agent.AgentTask
	SubmitAt time.Time
}

// TaskResult represents the result of task execution
type TaskResult struct {
	JobID  string
	TaskID string
	Output *agent.AgentOutput
	Error  error
}

// NewTaskScheduler creates a new task scheduler
func NewTaskScheduler(executorMgr executor.ExecutorManager, mode SchedulingMode) *TaskSchedulerImpl {
	ctx, cancel := context.WithCancel(context.Background())

	scheduler := &TaskSchedulerImpl{
		executorManager:     executorMgr,
		taskQueue:           make(chan *TaskSubmission, 10000),
		activeTasks:         make(map[string]*agent.AgentTask),
		taskResults:         make(chan *TaskResult, 1000),
		ctx:                 ctx,
		cancel:              cancel,
		maxLocalityWaitTime: 3 * time.Second,
		schedulingMode:      mode,
	}

	// Start scheduling loop
	go scheduler.scheduleLoop()
	go scheduler.resultLoop()

	return scheduler
}

// SetDAGScheduler sets the reference to DAGScheduler for event notification
func (ts *TaskSchedulerImpl) SetDAGScheduler(dagScheduler *DAGScheduler) {
	ts.dagScheduler = dagScheduler
}

// SubmitTask submits a task for execution
func (ts *TaskSchedulerImpl) SubmitTask(ctx context.Context, jobID string, task *agent.AgentTask) error {
	submission := &TaskSubmission{
		JobID:    jobID,
		Task:     task,
		SubmitAt: time.Now(),
	}

	select {
	case ts.taskQueue <- submission:
		ts.mu.Lock()
		ts.activeTasks[task.ID] = task
		ts.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// scheduleLoop continuously schedules tasks to executors
func (ts *TaskSchedulerImpl) scheduleLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	pendingTasks := make([]*TaskSubmission, 0)

	for {
		select {
		case <-ts.ctx.Done():
			return

		case submission := <-ts.taskQueue:
			pendingTasks = append(pendingTasks, submission)

		case <-ticker.C:
			if len(pendingTasks) == 0 {
				continue
			}

			// Get available executors
			executors := ts.executorManager.GetAvailableExecutors()
			if len(executors) == 0 {
				continue
			}

			// Try to schedule tasks with locality preferences
			remainingTasks := make([]*TaskSubmission, 0)

			for _, submission := range pendingTasks {
				scheduled := ts.tryScheduleTask(submission, executors)
				if !scheduled {
					remainingTasks = append(remainingTasks, submission)
				}
			}

			pendingTasks = remainingTasks
		}
	}
}

// tryScheduleTask attempts to schedule a task to an executor with locality awareness
func (ts *TaskSchedulerImpl) tryScheduleTask(submission *TaskSubmission, executors []executor.Executor) bool {
	task := submission.Task

	// Try locality levels in order of preference
	localityLevels := []LocalityLevel{ProcessLocal, NodeLocal, RackLocal, Any}

	for _, level := range localityLevels {
		// Check if we should wait for better locality
		waitTime := time.Since(submission.SubmitAt)
		if level != Any && waitTime < ts.maxLocalityWaitTime {
			// Skip this level for now, wait for better locality
			if level == RackLocal || level == NodeLocal {
				continue
			}
		}

		// Find executor matching this locality level
		exec := ts.findExecutorForLocality(task, executors, level)
		if exec != nil {
			// Launch task on executor
			go ts.launchTask(submission.JobID, task, exec)
			return true
		}
	}

	return false
}

// findExecutorForLocality finds an executor matching the locality requirement
func (ts *TaskSchedulerImpl) findExecutorForLocality(
	task *agent.AgentTask,
	executors []executor.Executor,
	level LocalityLevel,
) executor.Executor {

	preferredLoc := task.PreferredLoc

	for _, exec := range executors {
		if !exec.IsAvailable() {
			continue
		}

		switch level {
		case ProcessLocal:
			// Check if executor has the same partition
			if exec.Partition() == preferredLoc {
				return exec
			}
		case NodeLocal:
			// Check if executor is on the same node
			if exec.Node() == preferredLoc {
				return exec
			}
		case RackLocal:
			// Check if executor is in the same rack/region
			if exec.Rack() == preferredLoc {
				return exec
			}
		case Any:
			// Any available executor
			return exec
		}
	}

	return nil
}

// launchTask launches a task on an executor
func (ts *TaskSchedulerImpl) launchTask(jobID string, task *agent.AgentTask, exec executor.Executor) {
	task.State = agent.TaskRunning
	task.StartedAt = time.Now()

	// Execute task on executor
	output, err := exec.ExecuteTask(ts.ctx, task)

	result := &TaskResult{
		JobID:  jobID,
		TaskID: task.ID,
		Output: output,
		Error:  err,
	}

	// Send result back
	ts.taskResults <- result
}

// resultLoop processes task results
func (ts *TaskSchedulerImpl) resultLoop() {
	for {
		select {
		case <-ts.ctx.Done():
			return

		case result := <-ts.taskResults:
			ts.handleTaskResult(result)
		}
	}
}

// handleTaskResult processes a task result
func (ts *TaskSchedulerImpl) handleTaskResult(result *TaskResult) {
	ts.mu.Lock()
	task, exists := ts.activeTasks[result.TaskID]
	if !exists {
		ts.mu.Unlock()
		return
	}

	if result.Error != nil {
		task.State = agent.TaskFailed
		delete(ts.activeTasks, result.TaskID)
		ts.mu.Unlock()

		// Notify DAGScheduler
		if ts.dagScheduler != nil {
			ts.dagScheduler.eventQueue <- &TaskFailedEvent{
				JobID:  result.JobID,
				TaskID: result.TaskID,
				Error:  result.Error,
			}
		}
	} else {
		task.State = agent.TaskCompleted
		task.Output = result.Output
		task.CompletedAt = time.Now()
		delete(ts.activeTasks, result.TaskID)
		ts.mu.Unlock()

		// Notify DAGScheduler
		if ts.dagScheduler != nil {
			ts.dagScheduler.eventQueue <- &TaskCompletedEvent{
				JobID:  result.JobID,
				TaskID: result.TaskID,
				Output: result.Output,
			}
		}
	}
}

// GetTaskStatus returns the status of a task
func (ts *TaskSchedulerImpl) GetTaskStatus(taskID string) (*agent.AgentTask, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	task, exists := ts.activeTasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task %s not found", taskID)
	}

	return task, nil
}

// Stop gracefully shuts down the task scheduler
func (ts *TaskSchedulerImpl) Stop() error {
	ts.cancel()
	return nil
}

// GetMetrics returns scheduling metrics
func (ts *TaskSchedulerImpl) GetMetrics() *SchedulerMetrics {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return &SchedulerMetrics{
		ActiveTasks:   len(ts.activeTasks),
		QueuedTasks:   len(ts.taskQueue),
		TotalExecutors: ts.executorManager.GetExecutorCount(),
	}
}

// SchedulerMetrics holds scheduler statistics
type SchedulerMetrics struct {
	ActiveTasks    int
	QueuedTasks    int
	TotalExecutors int
}
