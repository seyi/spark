// Package scheduler provides metadata storage implementations
package scheduler

import (
	"encoding/json"
	"fmt"
	"sync"
)

// MemoryMetadataStore implements in-memory metadata storage
type MemoryMetadataStore struct {
	jobs         map[string]*Job
	stageResults map[string]map[int]*StageResult // jobID -> stageID -> result
	mu           sync.RWMutex
}

func NewMemoryMetadataStore() *MemoryMetadataStore {
	return &MemoryMetadataStore{
		jobs:         make(map[string]*Job),
		stageResults: make(map[string]map[int]*StageResult),
	}
}

func (m *MemoryMetadataStore) SaveJob(job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy to avoid mutations
	jobCopy := *job
	m.jobs[job.ID] = &jobCopy
	return nil
}

func (m *MemoryMetadataStore) GetJob(jobID string) (*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	job, exists := m.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	// Return copy
	jobCopy := *job
	return &jobCopy, nil
}

func (m *MemoryMetadataStore) SaveStageResult(jobID string, result *StageResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.stageResults[jobID]; !exists {
		m.stageResults[jobID] = make(map[int]*StageResult)
	}

	resultCopy := *result
	m.stageResults[jobID][result.StageID] = &resultCopy
	return nil
}

func (m *MemoryMetadataStore) GetStageResult(jobID string, stageID int) (*StageResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	jobResults, exists := m.stageResults[jobID]
	if !exists {
		return nil, fmt.Errorf("no results for job %s", jobID)
	}

	result, exists := jobResults[stageID]
	if !exists {
		return nil, fmt.Errorf("stage %d result not found for job %s", stageID, jobID)
	}

	resultCopy := *result
	return &resultCopy, nil
}

// FileMetadataStore implements file-based metadata storage
type FileMetadataStore struct {
	basePath string
	mu       sync.RWMutex
}

func NewFileMetadataStore(basePath string) *FileMetadataStore {
	return &FileMetadataStore{
		basePath: basePath,
	}
}

func (f *FileMetadataStore) SaveJob(job *Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// In a real implementation, write to file system
	_ = data
	return nil
}

func (f *FileMetadataStore) GetJob(jobID string) (*Job, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// In a real implementation, read from file system
	return nil, fmt.Errorf("not implemented")
}

func (f *FileMetadataStore) SaveStageResult(jobID string, result *StageResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal stage result: %w", err)
	}

	// In a real implementation, write to file system
	_ = data
	return nil
}

func (f *FileMetadataStore) GetStageResult(jobID string, stageID int) (*StageResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// In a real implementation, read from file system
	return nil, fmt.Errorf("not implemented")
}
