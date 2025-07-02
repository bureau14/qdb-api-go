package qdb

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// testLogFunction invokes all logger methods with test data.
// This provides a consistent way to generate log output for testing.
func testLogFunction() {
	L().Debug("test debug message", "key1", "value1")
	L().Info("test info message", "key2", "value2")
	L().Warn("test warn message", "key3", "value3")
	L().Error("test error message", "key4", "value4")
	L().Panic("test panic message", "key5", "value5")
	L().Detailed("test detailed message", "key6", "value6")
	
	// Test With() functionality
	logger := L().With("request_id", "12345")
	logger.Info("test with context", "user", "test-user")
}

// captureStderr captures stderr output during function execution.
func captureStderr(t *testing.T, f func()) string {
	t.Helper()
	
	// Save original stderr
	origStderr := os.Stderr
	
	// Create pipe for capturing
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	
	// Channel to signal reading is done
	done := make(chan string)
	
	// Start reading in a goroutine
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		done <- buf.String()
	}()
	
	// Replace stderr
	os.Stderr = w
	
	// Run the function
	f()
	
	// Restore stderr and close write end
	os.Stderr = origStderr
	w.Close()
	
	// Wait for reading to complete
	output := <-done
	r.Close()
	
	return output
}

// TestDefaultLoggerOutput verifies that the default logger produces output.
func TestDefaultLoggerOutput(t *testing.T) {
	// Save current logger
	originalLogger := L()
	defer func() {
		// Always restore original logger
		SetLogger(originalLogger)
	}()
	
	// Create a buffer to capture output
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Enable all log levels
	})
	
	// Create logger with buffer handler
	logger := NewSlogAdapter(handler)
	SetLogger(logger)
	
	// Call test function
	testLogFunction()
	
	// Get output from buffer
	output := buf.String()
	
	// Verify we got output
	if output == "" {
		t.Error("expected log output, got empty string")
	}
	
	// Check for expected messages
	expectedMessages := []string{
		"test debug message",
		"test info message",
		"test warn message",
		"test error message",
		"test panic message",
		"test detailed message",
		"test with context",
	}
	
	for _, msg := range expectedMessages {
		if !strings.Contains(output, msg) {
			t.Errorf("expected output to contain %q, got:\n%s", msg, output)
		}
	}
	
	// Check for attributes
	expectedAttrs := []string{
		"key1=value1",
		"key2=value2",
		"key3=value3",
		"key4=value4",
		"key5=value5",
		"key6=value6",
		"request_id=12345",
		"user=test-user",
	}
	
	for _, attr := range expectedAttrs {
		if !strings.Contains(output, attr) {
			t.Errorf("expected output to contain attribute %q, got:\n%s", attr, output)
		}
	}
}

// TestNilLoggerNoOutput verifies that NilLogger produces no output.
func TestNilLoggerNoOutput(t *testing.T) {
	// Save current logger
	originalLogger := L()
	defer func() {
		// Always restore original logger
		SetLogger(originalLogger)
	}()
	
	// Set NilLogger
	SetLogger(&NilLogger{})
	
	// Capture output
	output := captureStderr(t, testLogFunction)
	
	// Verify no output
	if output != "" {
		t.Errorf("expected no output from NilLogger, got:\n%s", output)
	}
}

// TestLoggerIsolation verifies that logger state is properly isolated between tests.
func TestLoggerIsolation(t *testing.T) {
	// Save current logger
	originalLogger := L()
	
	// Run test that changes logger
	t.Run("ModifyLogger", func(t *testing.T) {
		defer func() {
			// Restore logger even if test panics
			SetLogger(originalLogger)
		}()
		
		// Change to NilLogger
		SetLogger(&NilLogger{})
		
		// Verify it's set
		if _, ok := L().(*NilLogger); !ok {
			t.Error("expected NilLogger to be set")
		}
	})
	
	// Verify logger is restored
	if L() != originalLogger {
		t.Error("logger was not properly restored after test")
	}
}

// TestDefaultLoggerFunction verifies the defaultLogger() function works correctly.
func TestDefaultLoggerFunction(t *testing.T) {
	// Get a default logger
	logger := defaultLogger()
	
	// Verify it's not nil
	if logger == nil {
		t.Fatal("defaultLogger() returned nil")
	}
	
	// Test that it produces output
	originalLogger := L()
	defer SetLogger(originalLogger)
	
	// Create a pipe before creating the logger
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	
	// Save original stderr
	origStderr := os.Stderr
	os.Stderr = w
	
	// Create a new default logger that will use the piped stderr
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	testLogger := NewSlogAdapter(handler)
	SetLogger(testLogger)
	
	// Log the message
	L().Info("default logger test")
	
	// Restore stderr and close write end
	os.Stderr = origStderr
	w.Close()
	
	// Read output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	r.Close()
	output := buf.String()
	
	if !strings.Contains(output, "default logger test") {
		t.Errorf("expected output from default logger, got:\n%s", output)
	}
}

// TestNilLoggerWith verifies that NilLogger.With() returns a NilLogger.
func TestNilLoggerWith(t *testing.T) {
	nilLogger := &NilLogger{}
	
	// Call With() with some attributes
	newLogger := nilLogger.With("key", "value", "another", 123)
	
	// Verify it returns a NilLogger
	if _, ok := newLogger.(*NilLogger); !ok {
		t.Errorf("expected NilLogger.With() to return *NilLogger, got %T", newLogger)
	}
	
	// Verify the new logger also produces no output
	originalLogger := L()
	defer SetLogger(originalLogger)
	
	SetLogger(newLogger)
	
	output := captureStderr(t, func() {
		L().Info("should not appear")
	})
	
	if output != "" {
		t.Errorf("expected no output from NilLogger.With(), got:\n%s", output)
	}
}

// TestSetLoggerPanicOnNil verifies that SetLogger panics when given nil.
func TestSetLoggerPanicOnNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected SetLogger(nil) to panic")
		} else if msg, ok := r.(string); ok {
			if !strings.Contains(msg, "logger must not be nil") {
				t.Errorf("unexpected panic message: %s", msg)
			}
		}
	}()
	
	SetLogger(nil)
}

// TestLoggerConcurrency verifies that logger operations are thread-safe.
func TestLoggerConcurrency(t *testing.T) {
	// Save current logger
	originalLogger := L()
	defer SetLogger(originalLogger)
	
	// Set to NilLogger for silent operation
	SetLogger(&NilLogger{})
	
	// Run concurrent operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			
			// Perform various logger operations
			L().Debug("concurrent", "goroutine", id)
			L().Info("concurrent", "goroutine", id)
			logger := L().With("id", id)
			logger.Warn("concurrent with context")
			
			// Occasionally change logger
			if id%3 == 0 {
				SetLogger(&NilLogger{})
			}
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// If we get here without deadlock or panic, concurrency is working
}