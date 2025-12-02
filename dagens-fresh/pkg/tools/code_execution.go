package tools

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// CodeExecutionConfig configures the code execution sandbox
type CodeExecutionConfig struct {
	// Maximum execution time (default: 30s)
	Timeout time.Duration

	// Maximum memory in MB (default: 256MB)
	MaxMemoryMB int

	// Allowed languages (default: python)
	AllowedLanguages []string

	// Docker image per language
	DockerImages map[string]string

	// Enable network access (default: false for security)
	AllowNetwork bool

	// Enable file system writes (default: false for security)
	AllowFileWrites bool

	// Maximum output size in bytes (default: 10MB)
	MaxOutputSize int
}

// CodeExecutionResult contains execution results
type CodeExecutionResult struct {
	Stdout        string
	Stderr        string
	ExitCode      int
	ExecutionTime time.Duration
	Language      string
	Truncated     bool
	Error         string
}

// DefaultCodeExecutionConfig returns secure default configuration
func DefaultCodeExecutionConfig() CodeExecutionConfig {
	return CodeExecutionConfig{
		Timeout:          30 * time.Second,
		MaxMemoryMB:      256,
		AllowedLanguages: []string{"python", "javascript", "go"},
		DockerImages: map[string]string{
			"python":     "python:3.11-alpine",
			"javascript": "node:20-alpine",
			"go":         "golang:1.21-alpine",
		},
		AllowNetwork:    false,
		AllowFileWrites: false,
		MaxOutputSize:   10 * 1024 * 1024, // 10MB
	}
}

// CodeExecutionTool creates a secure code execution sandbox tool
// Inspired by ADK's geminitool.CodeExecution but designed for distributed systems
func CodeExecutionTool(config CodeExecutionConfig) *ToolDefinition {
	// Apply defaults if not set
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxMemoryMB == 0 {
		config.MaxMemoryMB = 256
	}
	if len(config.AllowedLanguages) == 0 {
		config.AllowedLanguages = []string{"python"}
	}
	if config.DockerImages == nil {
		config.DockerImages = DefaultCodeExecutionConfig().DockerImages
	}
	if config.MaxOutputSize == 0 {
		config.MaxOutputSize = 10 * 1024 * 1024
	}

	return &ToolDefinition{
		Name:        "execute_code",
		Description: "Execute code in a secure sandboxed environment. Supports Python, JavaScript, and Go. Use for calculations, data processing, algorithm implementation, and verification.",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"code": map[string]interface{}{
						"type":        "string",
						"description": "The code to execute",
					},
					"language": map[string]interface{}{
						"type":        "string",
						"description": "Programming language (python, javascript, go)",
						"enum":        config.AllowedLanguages,
					},
					"timeout": map[string]interface{}{
						"type":        "integer",
						"description": "Optional execution timeout in seconds (max: 60)",
					},
				},
				"required": []string{"code", "language"},
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			return executeCodeHandler(ctx, params, config)
		},
		Enabled: true,
	}
}

// executeCodeHandler handles code execution requests
func executeCodeHandler(ctx context.Context, params map[string]interface{}, config CodeExecutionConfig) (interface{}, error) {
	// Extract parameters
	code, ok := params["code"].(string)
	if !ok || code == "" {
		return nil, fmt.Errorf("code parameter is required and must be a non-empty string")
	}

	language, ok := params["language"].(string)
	if !ok || language == "" {
		return nil, fmt.Errorf("language parameter is required")
	}

	// Validate language is allowed
	languageAllowed := false
	for _, lang := range config.AllowedLanguages {
		if lang == language {
			languageAllowed = true
			break
		}
	}
	if !languageAllowed {
		return nil, fmt.Errorf("language %s not allowed. Allowed languages: %v", language, config.AllowedLanguages)
	}

	// Get timeout (use config default or override)
	timeout := config.Timeout
	if timeoutSec, ok := params["timeout"].(float64); ok {
		customTimeout := time.Duration(timeoutSec) * time.Second
		// Cap at 60 seconds for security
		if customTimeout > 60*time.Second {
			customTimeout = 60 * time.Second
		}
		timeout = customTimeout
	}

	// Execute code in sandbox
	startTime := time.Now()
	result, err := executeInDockerSandbox(ctx, code, language, timeout, config)
	result.ExecutionTime = time.Since(startTime)

	if err != nil {
		result.Error = err.Error()
	}

	// Return structured result
	return map[string]interface{}{
		"success":        err == nil && result.ExitCode == 0,
		"stdout":         result.Stdout,
		"stderr":         result.Stderr,
		"exit_code":      result.ExitCode,
		"execution_time": result.ExecutionTime.Seconds(),
		"language":       result.Language,
		"truncated":      result.Truncated,
		"error":          result.Error,
	}, nil
}

// executeInDockerSandbox runs code in an isolated Docker container
func executeInDockerSandbox(ctx context.Context, code, language string, timeout time.Duration, config CodeExecutionConfig) (*CodeExecutionResult, error) {
	result := &CodeExecutionResult{
		Language: language,
	}

	// Get Docker image for language
	image, ok := config.DockerImages[language]
	if !ok {
		return result, fmt.Errorf("no Docker image configured for language: %s", language)
	}

	// Build Docker command based on language
	var cmd *exec.Cmd
	switch language {
	case "python":
		cmd = buildPythonDockerCommand(code, image, timeout, config)
	case "javascript":
		cmd = buildJavaScriptDockerCommand(code, image, timeout, config)
	case "go":
		cmd = buildGoDockerCommand(code, image, timeout, config)
	default:
		return result, fmt.Errorf("unsupported language: %s", language)
	}

	// Create context with timeout
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Run command
	output, err := cmd.CombinedOutput()

	// Check if timeout occurred
	if execCtx.Err() == context.DeadlineExceeded {
		result.Stderr = fmt.Sprintf("Execution timed out after %v", timeout)
		result.ExitCode = 124 // Standard timeout exit code
		result.Error = "timeout"
		return result, fmt.Errorf("execution timeout")
	}

	// Parse output
	outputStr := string(output)

	// Truncate if too large
	if len(outputStr) > config.MaxOutputSize {
		outputStr = outputStr[:config.MaxOutputSize]
		result.Truncated = true
	}

	// Split stdout/stderr (basic approach - Docker combines them)
	if err != nil {
		result.Stderr = outputStr
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			result.ExitCode = 1
		}
		return result, err
	}

	result.Stdout = outputStr
	result.ExitCode = 0
	return result, nil
}

// buildPythonDockerCommand constructs Docker command for Python execution
func buildPythonDockerCommand(code, image string, timeout time.Duration, config CodeExecutionConfig) *exec.Cmd {
	args := []string{
		"run",
		"--rm",                                      // Remove container after execution
		"--network=none",                            // Disable network for security
		fmt.Sprintf("--memory=%dm", config.MaxMemoryMB), // Memory limit
		"--cpus=0.5",                                // CPU limit
		"--read-only",                               // Read-only filesystem
		"--tmpfs=/tmp:rw,noexec,nosuid,size=10m",    // Temporary space (no execution)
		"--security-opt=no-new-privileges",          // Security hardening
		"--cap-drop=ALL",                            // Drop all capabilities
		image,
		"python3", "-c", code,
	}

	// Allow network if configured
	if config.AllowNetwork {
		for i, arg := range args {
			if arg == "--network=none" {
				args[i] = "--network=bridge"
				break
			}
		}
	}

	return exec.Command("docker", args...)
}

// buildJavaScriptDockerCommand constructs Docker command for JavaScript execution
func buildJavaScriptDockerCommand(code, image string, timeout time.Duration, config CodeExecutionConfig) *exec.Cmd {
	args := []string{
		"run",
		"--rm",
		"--network=none",
		fmt.Sprintf("--memory=%dm", config.MaxMemoryMB),
		"--cpus=0.5",
		"--read-only",
		"--tmpfs=/tmp:rw,noexec,nosuid,size=10m",
		"--security-opt=no-new-privileges",
		"--cap-drop=ALL",
		image,
		"node", "-e", code,
	}

	if config.AllowNetwork {
		for i, arg := range args {
			if arg == "--network=none" {
				args[i] = "--network=bridge"
				break
			}
		}
	}

	return exec.Command("docker", args...)
}

// buildGoDockerCommand constructs Docker command for Go execution
func buildGoDockerCommand(code, image string, timeout time.Duration, config CodeExecutionConfig) *exec.Cmd {
	// For Go, we need to write to a file and compile
	// This is more complex, so we'll use a simpler approach with go run
	wrappedCode := fmt.Sprintf(`package main

%s
`, code)

	args := []string{
		"run",
		"--rm",
		"--network=none",
		fmt.Sprintf("--memory=%dm", config.MaxMemoryMB),
		"--cpus=0.5",
		"--tmpfs=/tmp:rw,size=50m", // Go needs temp space for compilation
		"--security-opt=no-new-privileges",
		"--cap-drop=ALL",
		image,
		"sh", "-c",
		fmt.Sprintf("echo '%s' > /tmp/main.go && cd /tmp && go run main.go", escapeForShell(wrappedCode)),
	}

	if config.AllowNetwork {
		for i, arg := range args {
			if arg == "--network=none" {
				args[i] = "--network=bridge"
				break
			}
		}
	}

	return exec.Command("docker", args...)
}

// escapeForShell escapes code for safe shell execution
func escapeForShell(code string) string {
	// Simple escaping - replace single quotes
	return strings.ReplaceAll(code, "'", "'\"'\"'")
}

// SimpleCodeExecutionTool creates a basic code execution tool with defaults
// Use this for quick setup without custom configuration
func SimpleCodeExecutionTool() *ToolDefinition {
	return CodeExecutionTool(DefaultCodeExecutionConfig())
}

// PythonOnlyCodeExecutionTool creates a Python-only code execution tool
// More restrictive, suitable for math/data analysis use cases
func PythonOnlyCodeExecutionTool() *ToolDefinition {
	config := DefaultCodeExecutionConfig()
	config.AllowedLanguages = []string{"python"}
	return CodeExecutionTool(config)
}

// RegisterCodeExecutionTools registers code execution tools in the registry
func RegisterCodeExecutionTools(registry *ToolRegistry, config CodeExecutionConfig) error {
	return registry.Register(CodeExecutionTool(config))
}
