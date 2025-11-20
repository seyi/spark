// Package scheduler implements distributed agent scheduling
// inspired by Apache Spark's DAGScheduler and TaskScheduler.
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/agent"
	"github.com/google/uuid"
)

// DAGScheduler is responsible for high-level scheduling of agent workflows.
// It builds DAGs, breaks them into stages, and submits tasks to TaskScheduler.
// Inspired by Spark's DAGScheduler.
type DAGScheduler struct {
	taskScheduler  TaskScheduler
	eventQueue     chan SchedulerEvent
	activeJobs     map[string]*Job
	stageResults   map[int]*StageResult
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	metadataStore  MetadataStore
}

// Job represents a complete agent workflow
type Job struct {
	ID        string
	DAG       *agent.AgentDAG
	StartTime time.Time
	EndTime   time.Time
	State     JobState
	Error     error
	Results   map[int]*StageResult
}

// JobState represents the execution state of a job
type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobCompleted
	JobFailed
)

// StageResult holds the output of a completed stage
type StageResult struct {
	StageID   int
	Outputs   map[string]*agent.AgentOutput // TaskID -> Output
	StartTime time.Time
	EndTime   time.Time
	Error     error
}

// SchedulerEvent represents events in the scheduling system
type SchedulerEvent interface {
	Type() EventType
}

type EventType int

const (
	EventJobSubmitted EventType = iota
	EventStageCompleted
	EventStageFailed
	EventTaskCompleted
	EventTaskFailed
)

type JobSubmittedEvent struct {
	Job *Job
}

func (e *JobSubmittedEvent) Type() EventType { return EventJobSubmitted }

type StageCompletedEvent struct {
	JobID   string
	StageID int
	Result  *StageResult
}

func (e *StageCompletedEvent) Type() EventType { return EventStageCompleted }

type StageFailedEvent struct {
	JobID   string
	StageID int
	Error   error
}

func (e *StageFailedEvent) Type() EventType { return EventStageFailed }

type TaskCompletedEvent struct {
	JobID  string
	TaskID string
	Output *agent.AgentOutput
}

func (e *TaskCompletedEvent) Type() EventType { return EventTaskCompleted }

type TaskFailedEvent struct {
	JobID  string
	TaskID string
	Error  error
}

func (e *TaskFailedEvent) Type() EventType { return EventTaskFailed }

// MetadataStore tracks job and stage metadata
type MetadataStore interface {
	SaveJob(job *Job) error
	GetJob(jobID string) (*Job, error)
	SaveStageResult(jobID string, result *StageResult) error
	GetStageResult(jobID string, stageID int) (*StageResult, error)
}

// NewDAGScheduler creates a new DAG scheduler
func NewDAGScheduler(taskScheduler TaskScheduler, store MetadataStore) *DAGScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &DAGScheduler{
		taskScheduler: taskScheduler,
		eventQueue:    make(chan SchedulerEvent, 1000),
		activeJobs:    make(map[string]*Job),
		stageResults:  make(map[int]*StageResult),
		ctx:           ctx,
		cancel:        cancel,
		metadataStore: store,
	}

	// Start event loop
	go scheduler.eventLoop()

	return scheduler
}

// SubmitJob submits a new agent workflow for execution
func (s *DAGScheduler) SubmitJob(root agent.Agent, input *agent.AgentInput) (string, error) {
	// Build DAG from agent dependencies
	dag, err := agent.NewAgentDAG(root, input)
	if err != nil {
		return "", fmt.Errorf("failed to build DAG: %w", err)
	}

	job := &Job{
		ID:        uuid.New().String(),
		DAG:       dag,
		StartTime: time.Now(),
		State:     JobPending,
		Results:   make(map[int]*StageResult),
	}

	s.mu.Lock()
	s.activeJobs[job.ID] = job
	s.mu.Unlock()

	// Save job metadata
	if err := s.metadataStore.SaveJob(job); err != nil {
		return "", fmt.Errorf("failed to save job metadata: %w", err)
	}

	// Submit job event
	s.eventQueue <- &JobSubmittedEvent{Job: job}

	return job.ID, nil
}

// eventLoop processes scheduler events
func (s *DAGScheduler) eventLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-s.eventQueue:
			s.handleEvent(event)
		}
	}
}

// handleEvent processes a single scheduler event
func (s *DAGScheduler) handleEvent(event SchedulerEvent) {
	switch e := event.(type) {
	case *JobSubmittedEvent:
		s.handleJobSubmitted(e)
	case *StageCompletedEvent:
		s.handleStageCompleted(e)
	case *StageFailedEvent:
		s.handleStageFailed(e)
	case *TaskCompletedEvent:
		s.handleTaskCompleted(e)
	case *TaskFailedEvent:
		s.handleTaskFailed(e)
	}
}

// handleJobSubmitted processes a new job submission
func (s *DAGScheduler) handleJobSubmitted(event *JobSubmittedEvent) {
	job := event.Job
	job.State = JobRunning

	// Start with the deepest stages (those with no dependencies)
	// In our stage ordering, higher stage IDs are executed first
	for i := len(job.DAG.Stages) - 1; i >= 0; i-- {
		stage := job.DAG.Stages[i]
		if s.areStageDependenciesMet(job, stage) {
			s.submitStage(job, stage)
		}
	}
}

// areStageDependenciesMet checks if all stage dependencies are completed
func (s *DAGScheduler) areStageDependenciesMet(job *Job, stage *agent.AgentStage) bool {
	for _, depStage := range stage.Dependencies {
		result, exists := job.Results[depStage.ID]
		if !exists || result.Error != nil {
			return false
		}
	}
	return true
}

// submitStage submits all tasks in a stage to the task scheduler
func (s *DAGScheduler) submitStage(job *Job, stage *agent.AgentStage) {
	stageResult := &StageResult{
		StageID:   stage.ID,
		Outputs:   make(map[string]*agent.AgentOutput),
		StartTime: time.Now(),
	}

	// Submit all tasks in this stage
	for _, task := range stage.Tasks {
		task.State = agent.TaskScheduled
		s.taskScheduler.SubmitTask(s.ctx, job.ID, task)
	}
}

// handleTaskCompleted processes task completion
func (s *DAGScheduler) handleTaskCompleted(event *TaskCompletedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.activeJobs[event.JobID]
	if !exists {
		return
	}

	// Find the stage containing this task
	var stage *agent.AgentStage
	for _, st := range job.DAG.Stages {
		for _, task := range st.Tasks {
			if task.ID == event.TaskID {
				stage = st
				task.State = agent.TaskCompleted
				task.Output = event.Output
				task.CompletedAt = time.Now()
				break
			}
		}
		if stage != nil {
			break
		}
	}

	if stage == nil {
		return
	}

	// Check if all tasks in stage are completed
	allCompleted := true
	for _, task := range stage.Tasks {
		if task.State != agent.TaskCompleted {
			allCompleted = false
			break
		}
	}

	if allCompleted {
		result := &StageResult{
			StageID:   stage.ID,
			Outputs:   make(map[string]*agent.AgentOutput),
			StartTime: time.Now(),
			EndTime:   time.Now(),
		}

		for _, task := range stage.Tasks {
			result.Outputs[task.ID] = task.Output
		}

		s.eventQueue <- &StageCompletedEvent{
			JobID:   event.JobID,
			StageID: stage.ID,
			Result:  result,
		}
	}
}

// handleStageCompleted processes stage completion
func (s *DAGScheduler) handleStageCompleted(event *StageCompletedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.activeJobs[event.JobID]
	if !exists {
		return
	}

	job.Results[event.StageID] = event.Result

	// Save stage result
	s.metadataStore.SaveStageResult(event.JobID, event.Result)

	// Check if we can submit more stages
	for _, stage := range job.DAG.Stages {
		if _, completed := job.Results[stage.ID]; !completed {
			if s.areStageDependenciesMet(job, stage) {
				s.submitStage(job, stage)
			}
		}
	}

	// Check if job is complete
	if len(job.Results) == len(job.DAG.Stages) {
		job.State = JobCompleted
		job.EndTime = time.Now()
		s.metadataStore.SaveJob(job)
	}
}

// handleTaskFailed processes task failure
func (s *DAGScheduler) handleTaskFailed(event *TaskFailedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.activeJobs[event.JobID]
	if !exists {
		return
	}

	// Find the task
	for _, stage := range job.DAG.Stages {
		for _, task := range stage.Tasks {
			if task.ID == event.TaskID {
				task.Attempt++
				if task.Attempt < task.Input.MaxRetries {
					// Retry the task
					task.State = agent.TaskRetrying
					s.taskScheduler.SubmitTask(s.ctx, job.ID, task)
				} else {
					// Max retries exceeded, fail the stage
					task.State = agent.TaskFailed
					s.eventQueue <- &StageFailedEvent{
						JobID:   event.JobID,
						StageID: stage.ID,
						Error:   event.Error,
					}
				}
				return
			}
		}
	}
}

// handleStageFailed processes stage failure
func (s *DAGScheduler) handleStageFailed(event *StageFailedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.activeJobs[event.JobID]
	if !exists {
		return
	}

	job.State = JobFailed
	job.Error = event.Error
	job.EndTime = time.Now()

	s.metadataStore.SaveJob(job)
}

// GetJobStatus returns the current status of a job
func (s *DAGScheduler) GetJobStatus(jobID string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.activeJobs[jobID]
	if !exists {
		return s.metadataStore.GetJob(jobID)
	}

	return job, nil
}

// Stop gracefully shuts down the scheduler
func (s *DAGScheduler) Stop() {
	s.cancel()
}
