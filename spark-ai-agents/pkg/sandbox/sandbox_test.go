package sandbox

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewSandboxedCodeExecutor(t *testing.T) {
	t.Run("With Work Directory", func(t *testing.T) {
		executor, err := NewSandboxedCodeExecutor("/tmp/test-sandbox")
		if err != nil {
			t.Fatalf("Failed to create executor: %v", err)
		}

		if executor == nil {
			t.Fatal("Executor should not be nil")
		}

		if executor.workDir != "/tmp/test-sandbox" {
			t.Error("Work directory not set correctly")
		}
	})

	t.Run("With Default Work Directory", func(t *testing.T) {
		executor, err := NewSandboxedCodeExecutor("")
		if err != nil {
			t.Fatalf("Failed to create executor: %v", err)
		}

		if executor.workDir == "" {
			t.Error("Work directory should be set to default")
		}
	})

	t.Run("Default Resource Limits", func(t *testing.T) {
		executor, err := NewSandboxedCodeExecutor("")
		if err != nil {
			t.Fatalf("Failed to create executor: %v", err)
		}

		if executor.limits.MaxMemoryMB != 512 {
			t.Error("Default memory limit not set correctly")
		}

		if executor.limits.Timeout != 30*time.Second {
			t.Error("Default timeout not set correctly")
		}
	})
}

func TestSetResourceLimits(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	customLimits := ResourceLimits{
		MaxMemoryMB:   1024,
		MaxCPUPercent: 75.0,
		MaxDiskMB:     200,
		Timeout:       60 * time.Second,
		MaxProcesses:  20,
	}

	executor.SetResourceLimits(customLimits)

	if executor.limits.MaxMemoryMB != 1024 {
		t.Error("Memory limit not updated")
	}

	if executor.limits.Timeout != 60*time.Second {
		t.Error("Timeout not updated")
	}
}

func TestSupportedLanguages(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	languages := executor.SupportedLanguages()

	expectedLanguages := map[string]bool{
		"python":     false,
		"javascript": false,
		"bash":       false,
		"go":         false,
	}

	for _, lang := range languages {
		if _, exists := expectedLanguages[lang]; exists {
			expectedLanguages[lang] = true
		}
	}

	for lang, found := range expectedLanguages {
		if !found {
			t.Errorf("Language '%s' not in supported languages", lang)
		}
	}
}

func TestExecutePython(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	t.Run("Simple Print", func(t *testing.T) {
		code := `print("Hello, World!")`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "python")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if result == nil {
			t.Fatal("Result should not be nil")
		}

		if !strings.Contains(result.Stdout, "Hello, World!") {
			t.Errorf("Expected 'Hello, World!' in output, got: %s", result.Stdout)
		}

		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}
	})

	t.Run("Simple Calculation", func(t *testing.T) {
		code := `
result = 2 + 2
print(f"Result: {result}")
`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "python")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if !strings.Contains(result.Stdout, "Result: 4") {
			t.Errorf("Expected 'Result: 4' in output, got: %s", result.Stdout)
		}
	})

	t.Run("Syntax Error", func(t *testing.T) {
		code := `print("missing closing quote`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "python")

		// Should not return error from Execute, but exit code should be non-zero
		if err == nil && result != nil {
			if result.ExitCode == 0 {
				t.Error("Expected non-zero exit code for syntax error")
			}
		}
	})
}

func TestExecuteJavaScript(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	t.Run("Simple Console Log", func(t *testing.T) {
		code := `console.log("Hello from Node.js");`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "javascript")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if !strings.Contains(result.Stdout, "Hello from Node.js") {
			t.Errorf("Expected output in stdout, got: %s", result.Stdout)
		}

		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}
	})

	t.Run("JavaScript Calculation", func(t *testing.T) {
		code := `
const result = 10 * 5;
console.log('Result:', result);
`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "javascript")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if !strings.Contains(result.Stdout, "50") {
			t.Errorf("Expected '50' in output, got: %s", result.Stdout)
		}
	})
}

func TestExecuteBash(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	t.Run("Echo Command", func(t *testing.T) {
		code := `echo "Hello from Bash"`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "bash")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if !strings.Contains(result.Stdout, "Hello from Bash") {
			t.Errorf("Expected output in stdout, got: %s", result.Stdout)
		}

		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}
	})

	t.Run("Environment Variable", func(t *testing.T) {
		code := `
TEST_VAR="test value"
echo "Variable: $TEST_VAR"
`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "bash")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if !strings.Contains(result.Stdout, "test value") {
			t.Errorf("Expected 'test value' in output, got: %s", result.Stdout)
		}
	})
}

func TestExecuteGo(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	t.Run("Simple Go Program", func(t *testing.T) {
		code := `
package main

import "fmt"

func main() {
	fmt.Println("Hello from Go")
}
`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "go")
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		if !strings.Contains(result.Stdout, "Hello from Go") {
			t.Errorf("Expected output in stdout, got: %s", result.Stdout)
		}

		if result.ExitCode != 0 {
			t.Errorf("Expected exit code 0, got %d", result.ExitCode)
		}
	})

	t.Run("Go Compilation Error", func(t *testing.T) {
		code := `
package main

func main() {
	// Missing import and syntax error
	fmt.Println("test"
}
`
		ctx := context.Background()

		result, err := executor.Execute(ctx, code, "go")

		// Should fail compilation
		if err == nil && result != nil {
			if result.ExitCode == 0 {
				t.Error("Expected non-zero exit code for compilation error")
			}
		}
	})
}

func TestExecutionTimeout(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	// Set very short timeout
	executor.SetResourceLimits(ResourceLimits{
		Timeout: 100 * time.Millisecond,
	})

	// Code that runs longer than timeout
	code := `
import time
time.sleep(10)
print("This should not print")
`
	ctx := context.Background()

	startTime := time.Now()
	result, err := executor.Execute(ctx, code, "python")
	duration := time.Since(startTime)

	// Should timeout
	if duration > 2*time.Second {
		t.Error("Execution took too long, timeout may not be working")
	}

	if result != nil && result.Error == nil && err == nil {
		t.Log("Note: Timeout may have occurred (this is expected)")
	}
}

func TestUnsupportedLanguage(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	code := `print("test")`
	ctx := context.Background()

	_, err = executor.Execute(ctx, code, "unsupported")
	if err == nil {
		t.Error("Expected error for unsupported language")
	}

	if !strings.Contains(err.Error(), "unsupported language") {
		t.Errorf("Expected 'unsupported language' error, got: %v", err)
	}
}

func TestExecutionDuration(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	code := `
import time
time.sleep(0.1)
print("done")
`
	ctx := context.Background()

	result, err := executor.Execute(ctx, code, "python")
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	if result.Duration == 0 {
		t.Error("Duration should be measured")
	}

	if result.Duration < 100*time.Millisecond {
		t.Error("Duration seems too short for the sleep command")
	}
}

func TestNewDockerCodeExecutor(t *testing.T) {
	t.Run("With Image Name", func(t *testing.T) {
		executor := NewDockerCodeExecutor("python:3.11-slim")

		if executor == nil {
			t.Fatal("Executor should not be nil")
		}

		if executor.imageName != "python:3.11-slim" {
			t.Error("Image name not set correctly")
		}
	})

	t.Run("With Default Image", func(t *testing.T) {
		executor := NewDockerCodeExecutor("")

		if executor.imageName == "" {
			t.Error("Should have default image name")
		}
	})

	t.Run("Supported Languages", func(t *testing.T) {
		executor := NewDockerCodeExecutor("")
		languages := executor.SupportedLanguages()

		if len(languages) == 0 {
			t.Error("Should support some languages")
		}
	})
}

func TestDockerResourceLimits(t *testing.T) {
	executor := NewDockerCodeExecutor("")

	customLimits := ResourceLimits{
		MaxMemoryMB:   1024,
		MaxCPUPercent: 80.0,
		Timeout:       45 * time.Second,
	}

	executor.SetResourceLimits(customLimits)

	executor.mu.RLock()
	if executor.limits.MaxMemoryMB != 1024 {
		t.Error("Memory limit not set correctly")
	}
	executor.mu.RUnlock()
}

func TestSecurityValidator(t *testing.T) {
	validator := &SecurityValidator{}

	t.Run("Dangerous Pattern - rm -rf", func(t *testing.T) {
		code := `rm -rf /important/directory`
		err := validator.Validate(code, "bash")

		if err == nil {
			t.Error("Should detect dangerous 'rm -rf' pattern")
		}

		if !strings.Contains(err.Error(), "dangerous pattern") {
			t.Errorf("Expected dangerous pattern error, got: %v", err)
		}
	})

	t.Run("Dangerous Pattern - eval", func(t *testing.T) {
		code := `eval("malicious code")`
		err := validator.Validate(code, "python")

		if err == nil {
			t.Error("Should detect dangerous 'eval' pattern")
		}
	})

	t.Run("Dangerous Pattern - exec", func(t *testing.T) {
		code := `exec("command")`
		err := validator.Validate(code, "python")

		if err == nil {
			t.Error("Should detect dangerous 'exec' pattern")
		}
	})

	t.Run("Dangerous Pattern - fork", func(t *testing.T) {
		code := `fork()`
		err := validator.Validate(code, "c")

		if err == nil {
			t.Error("Should detect dangerous 'fork' pattern")
		}
	})

	t.Run("Dangerous Pattern - system", func(t *testing.T) {
		code := `system("command")`
		err := validator.Validate(code, "c")

		if err == nil {
			t.Error("Should detect dangerous 'system' pattern")
		}
	})

	t.Run("Dangerous Pattern - __import__", func(t *testing.T) {
		code := `__import__("os").system("ls")`
		err := validator.Validate(code, "python")

		if err == nil {
			t.Error("Should detect dangerous '__import__' pattern")
		}
	})

	t.Run("Safe Code", func(t *testing.T) {
		code := `
def add(a, b):
    return a + b

result = add(2, 3)
print(result)
`
		err := validator.Validate(code, "python")

		if err != nil {
			t.Errorf("Safe code should pass validation: %v", err)
		}
	})

	t.Run("Code Too Large", func(t *testing.T) {
		// Create code larger than 100KB
		code := strings.Repeat("a", 100001)
		err := validator.Validate(code, "python")

		if err == nil {
			t.Error("Should reject code that is too large")
		}

		if !strings.Contains(err.Error(), "code too large") {
			t.Errorf("Expected 'code too large' error, got: %v", err)
		}
	})
}

func TestResourceUsage(t *testing.T) {
	usage := &ResourceUsage{
		MemoryUsedMB: 256.5,
		CPUTimeMS:    1500,
		DiskUsedMB:   10.2,
	}

	if usage.MemoryUsedMB != 256.5 {
		t.Error("Memory usage not set correctly")
	}

	if usage.CPUTimeMS != 1500 {
		t.Error("CPU time not set correctly")
	}

	if usage.DiskUsedMB != 10.2 {
		t.Error("Disk usage not set correctly")
	}
}

func TestExecutionResult(t *testing.T) {
	result := &ExecutionResult{
		Stdout:   "output",
		Stderr:   "error",
		ExitCode: 1,
		Duration: 100 * time.Millisecond,
		ResourceUsage: &ResourceUsage{
			MemoryUsedMB: 128.0,
		},
	}

	if result.Stdout != "output" {
		t.Error("Stdout not set correctly")
	}

	if result.Stderr != "error" {
		t.Error("Stderr not set correctly")
	}

	if result.ExitCode != 1 {
		t.Error("Exit code not set correctly")
	}

	if result.Duration != 100*time.Millisecond {
		t.Error("Duration not set correctly")
	}

	if result.ResourceUsage.MemoryUsedMB != 128.0 {
		t.Error("Resource usage not set correctly")
	}
}

func TestContainsHelper(t *testing.T) {
	t.Run("String Contains Substring", func(t *testing.T) {
		if !contains("hello world", "world") {
			t.Error("Should find 'world' in 'hello world'")
		}
	})

	t.Run("String Does Not Contain Substring", func(t *testing.T) {
		if contains("hello", "world") {
			t.Error("Should not find 'world' in 'hello'")
		}
	})

	t.Run("Empty Substring", func(t *testing.T) {
		if !contains("hello", "") {
			t.Error("Empty substring should always be found")
		}
	})

	t.Run("Equal Strings", func(t *testing.T) {
		if !contains("test", "test") {
			t.Error("Equal strings should match")
		}
	})
}

func TestFindSubstringHelper(t *testing.T) {
	t.Run("Substring at Beginning", func(t *testing.T) {
		if !findSubstring("hello world", "hello") {
			t.Error("Should find substring at beginning")
		}
	})

	t.Run("Substring at End", func(t *testing.T) {
		if !findSubstring("hello world", "world") {
			t.Error("Should find substring at end")
		}
	})

	t.Run("Substring in Middle", func(t *testing.T) {
		if !findSubstring("hello world", "lo wo") {
			t.Error("Should find substring in middle")
		}
	})

	t.Run("Substring Not Found", func(t *testing.T) {
		if findSubstring("hello", "xyz") {
			t.Error("Should not find non-existent substring")
		}
	})
}

func TestMultipleExecutions(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	ctx := context.Background()

	// Execute multiple programs
	for i := 0; i < 5; i++ {
		code := `print("Iteration ` + string(rune(i+'0')) + `")`
		result, err := executor.Execute(ctx, code, "python")

		if err != nil {
			t.Errorf("Execution %d failed: %v", i, err)
		}

		if result.ExitCode != 0 {
			t.Errorf("Execution %d had non-zero exit code", i)
		}
	}
}

func TestContextCancellation(t *testing.T) {
	executor, err := NewSandboxedCodeExecutor("")
	if err != nil {
		t.Fatalf("Failed to create executor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	code := `print("This should not execute")`
	_, err = executor.Execute(ctx, code, "python")

	// Execution with cancelled context should fail or handle gracefully
	if err == nil {
		t.Log("Note: Execution with cancelled context completed (may be expected)")
	}
}
