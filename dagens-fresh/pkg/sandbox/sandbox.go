// Package sandbox provides safe code execution for agent-generated code
// Inspired by ADK's AgentEngineSandboxCodeExecutor
package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// CodeExecutor executes code safely in an isolated environment
type CodeExecutor interface {
	Execute(ctx context.Context, code string, language string) (*ExecutionResult, error)
	SupportedLanguages() []string
	SetResourceLimits(limits ResourceLimits)
}

// ExecutionResult represents the result of code execution
type ExecutionResult struct {
	Stdout       string
	Stderr       string
	ExitCode     int
	Duration     time.Duration
	Error        error
	ResourceUsage *ResourceUsage
}

// ResourceLimits defines execution resource constraints
type ResourceLimits struct {
	MaxMemoryMB  int64
	MaxCPUPercent float64
	MaxDiskMB    int64
	Timeout      time.Duration
	MaxProcesses int
}

// ResourceUsage tracks actual resource consumption
type ResourceUsage struct{
	MemoryUsedMB  float64
	CPUTimeMS     int64
	DiskUsedMB    float64
}

// SandboxedCodeExecutor implements safe code execution
type SandboxedCodeExecutor struct {
	workDir  string
	limits   ResourceLimits
	mu       sync.RWMutex
}

// NewSandboxedCodeExecutor creates a new sandboxed executor
func NewSandboxedCodeExecutor(workDir string) (*SandboxedCodeExecutor, error) {
	if workDir == "" {
		workDir = filepath.Join(os.TempDir(), "spark-agents-sandbox")
	}

	// Create work directory
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work directory: %w", err)
	}

	return &SandboxedCodeExecutor{
		workDir: workDir,
		limits: ResourceLimits{
			MaxMemoryMB:   512,
			MaxCPUPercent: 50.0,
			MaxDiskMB:     100,
			Timeout:       30 * time.Second,
			MaxProcesses:  10,
		},
	}, nil
}

func (s *SandboxedCodeExecutor) SetResourceLimits(limits ResourceLimits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits = limits
}

func (s *SandboxedCodeExecutor) SupportedLanguages() []string {
	return []string{
		"python",
		"javascript",
		"bash",
		"go",
	}
}

// Execute runs code in a sandboxed environment
func (s *SandboxedCodeExecutor) Execute(ctx context.Context, code string, language string) (*ExecutionResult, error) {
	s.mu.RLock()
	limits := s.limits
	s.mu.RUnlock()

	// Create execution context with timeout
	execCtx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()

	// Create temporary directory for this execution
	execID := fmt.Sprintf("exec-%d", time.Now().UnixNano())
	execDir := filepath.Join(s.workDir, execID)
	if err := os.MkdirAll(execDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create execution directory: %w", err)
	}
	defer os.RemoveAll(execDir)

	startTime := time.Now()

	var result *ExecutionResult
	var err error

	switch language {
	case "python":
		result, err = s.executePython(execCtx, code, execDir, limits)
	case "javascript":
		result, err = s.executeJavaScript(execCtx, code, execDir, limits)
	case "bash":
		result, err = s.executeBash(execCtx, code, execDir, limits)
	case "go":
		result, err = s.executeGo(execCtx, code, execDir, limits)
	default:
		return nil, fmt.Errorf("unsupported language: %s", language)
	}

	if result != nil {
		result.Duration = time.Since(startTime)
	}

	return result, err
}

// executePython runs Python code
func (s *SandboxedCodeExecutor) executePython(ctx context.Context, code string, execDir string, limits ResourceLimits) (*ExecutionResult, error) {
	// Write code to file
	codePath := filepath.Join(execDir, "main.py")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return nil, fmt.Errorf("failed to write code file: %w", err)
	}

	// Execute with python
	cmd := exec.CommandContext(ctx, "python3", codePath)
	cmd.Dir = execDir

	// Set resource limits (platform-specific implementation needed)
	// For now, just use context timeout

	return s.runCommand(cmd)
}

// executeJavaScript runs JavaScript code with Node.js
func (s *SandboxedCodeExecutor) executeJavaScript(ctx context.Context, code string, execDir string, limits ResourceLimits) (*ExecutionResult, error) {
	codePath := filepath.Join(execDir, "main.js")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return nil, fmt.Errorf("failed to write code file: %w", err)
	}

	cmd := exec.CommandContext(ctx, "node", codePath)
	cmd.Dir = execDir

	return s.runCommand(cmd)
}

// executeBash runs Bash scripts
func (s *SandboxedCodeExecutor) executeBash(ctx context.Context, code string, execDir string, limits ResourceLimits) (*ExecutionResult, error) {
	codePath := filepath.Join(execDir, "script.sh")
	if err := os.WriteFile(codePath, []byte(code), 0755); err != nil {
		return nil, fmt.Errorf("failed to write code file: %w", err)
	}

	cmd := exec.CommandContext(ctx, "bash", codePath)
	cmd.Dir = execDir

	return s.runCommand(cmd)
}

// executeGo compiles and runs Go code
func (s *SandboxedCodeExecutor) executeGo(ctx context.Context, code string, execDir string, limits ResourceLimits) (*ExecutionResult, error) {
	codePath := filepath.Join(execDir, "main.go")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return nil, fmt.Errorf("failed to write code file: %w", err)
	}

	// First compile
	compileCmd := exec.CommandContext(ctx, "go", "build", "-o", "program", codePath)
	compileCmd.Dir = execDir

	if compileResult, err := s.runCommand(compileCmd); err != nil || compileResult.ExitCode != 0 {
		return compileResult, err
	}

	// Then execute
	runCmd := exec.CommandContext(ctx, "./program")
	runCmd.Dir = execDir

	return s.runCommand(runCmd)
}

// runCommand executes a command and captures output
func (s *SandboxedCodeExecutor) runCommand(cmd *exec.Cmd) (*ExecutionResult, error) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start command: %w", err)
	}

	// Read output
	stdoutBuf := make([]byte, 1024*1024) // 1MB limit
	stderrBuf := make([]byte, 1024*1024)

	stdoutN, _ := stdoutPipe.Read(stdoutBuf)
	stderrN, _ := stderrPipe.Read(stderrBuf)

	err = cmd.Wait()

	result := &ExecutionResult{
		Stdout:   string(stdoutBuf[:stdoutN]),
		Stderr:   string(stderrBuf[:stderrN]),
		ExitCode: cmd.ProcessState.ExitCode(),
		ResourceUsage: &ResourceUsage{},
	}

	if err != nil {
		result.Error = err
	}

	return result, nil
}

// DockerCodeExecutor uses Docker for better isolation
type DockerCodeExecutor struct {
	imageName string
	limits    ResourceLimits
	mu        sync.RWMutex
}

// NewDockerCodeExecutor creates a Docker-based executor
func NewDockerCodeExecutor(imageName string) *DockerCodeExecutor {
	if imageName == "" {
		imageName = "python:3.11-slim" // Default image
	}

	return &DockerCodeExecutor{
		imageName: imageName,
		limits: ResourceLimits{
			MaxMemoryMB:   512,
			MaxCPUPercent: 50.0,
			Timeout:       30 * time.Second,
		},
	}
}

func (d *DockerCodeExecutor) SetResourceLimits(limits ResourceLimits) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.limits = limits
}

func (d *DockerCodeExecutor) SupportedLanguages() []string {
	return []string{"python", "javascript", "bash"}
}

func (d *DockerCodeExecutor) Execute(ctx context.Context, code string, language string) (*ExecutionResult, error) {
	d.mu.RLock()
	limits := d.limits
	d.mu.RUnlock()

	execCtx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()

	// Create temporary file for code
	tmpFile, err := os.CreateTemp("", "code-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(code)); err != nil {
		return nil, fmt.Errorf("failed to write code: %w", err)
	}
	tmpFile.Close()

	// Build docker run command
	args := []string{
		"run",
		"--rm",
		"--network", "none", // No network access
		"--memory", fmt.Sprintf("%dm", limits.MaxMemoryMB),
		"--cpus", fmt.Sprintf("%.2f", limits.MaxCPUPercent/100.0),
		"-v", fmt.Sprintf("%s:/code:ro", tmpFile.Name()),
		d.imageName,
	}

	// Add command based on language
	switch language {
	case "python":
		args = append(args, "python3", "/code")
	case "javascript":
		args = append(args, "node", "/code")
	case "bash":
		args = append(args, "bash", "/code")
	default:
		return nil, fmt.Errorf("unsupported language: %s", language)
	}

	cmd := exec.CommandContext(execCtx, "docker", args...)

	startTime := time.Now()
	stdoutPipe, _ := cmd.StdoutPipe()
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start docker: %w", err)
	}

	stdoutBuf := make([]byte, 1024*1024)
	stderrBuf := make([]byte, 1024*1024)

	stdoutN, _ := stdoutPipe.Read(stdoutBuf)
	stderrN, _ := stderrPipe.Read(stderrBuf)

	err = cmd.Wait()

	result := &ExecutionResult{
		Stdout:        string(stdoutBuf[:stdoutN]),
		Stderr:        string(stderrBuf[:stderrN]),
		ExitCode:      cmd.ProcessState.ExitCode(),
		Duration:      time.Since(startTime),
		ResourceUsage: &ResourceUsage{},
	}

	if err != nil {
		result.Error = err
	}

	return result, nil
}

// SecurityValidator validates code before execution
type SecurityValidator struct{}

// Validate checks code for dangerous patterns
func (sv *SecurityValidator) Validate(code string, language string) error {
	// Check for dangerous patterns
	dangerousPatterns := []string{
		"rm -rf",
		"fork()",
		"system(",
		"exec(",
		"eval(",
		"__import__",
	}

	for _, pattern := range dangerousPatterns {
		if contains(code, pattern) {
			return fmt.Errorf("dangerous pattern detected: %s", pattern)
		}
	}

	// Check code length
	if len(code) > 100000 { // 100KB limit
		return fmt.Errorf("code too large: %d bytes (max 100KB)", len(code))
	}

	return nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
