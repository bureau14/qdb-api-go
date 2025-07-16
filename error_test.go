package qdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ------------------------------------------------------------------
// Is() method tests
// ------------------------------------------------------------------

func TestErrorType_Is_SameErrorType(t *testing.T) {
	err1 := ErrAliasNotFound
	err2 := ErrAliasNotFound

	assert.True(t, err1.Is(err2))
	assert.True(t, err2.Is(err1))
}

func TestErrorType_Is_DifferentErrorTypes(t *testing.T) {
	err1 := ErrAliasNotFound
	err2 := ErrAliasAlreadyExists

	assert.False(t, err1.Is(err2))
	assert.False(t, err2.Is(err1))
}

func TestErrorType_Is_NonErrorType(t *testing.T) {
	err1 := ErrAliasNotFound
	err2 := errors.New("standard error")

	assert.False(t, err1.Is(err2))
}

func TestErrorType_Is_WithStandardLibraryErrorsIs(t *testing.T) {
	err1 := ErrAliasNotFound
	err2 := ErrAliasNotFound
	err3 := ErrAliasAlreadyExists

	assert.True(t, errors.Is(err1, err2))
	assert.False(t, errors.Is(err1, err3))
}

func TestErrorType_Is_WithWrappedError(t *testing.T) {
	baseErr := ErrAliasNotFound
	wrappedErr := fmt.Errorf("operation failed: %w", baseErr)

	assert.True(t, errors.Is(wrappedErr, baseErr))
	assert.True(t, errors.Is(wrappedErr, ErrAliasNotFound))
}

func TestErrorType_Is_MultipleTypes(t *testing.T) {
	testCases := []struct {
		name string
		err1 ErrorType
		err2 ErrorType
		same bool
	}{
		{"same alias not found", ErrAliasNotFound, ErrAliasNotFound, true},
		{"same alias already exists", ErrAliasAlreadyExists, ErrAliasAlreadyExists, true},
		{"same timeout", ErrTimeout, ErrTimeout, true},
		{"same connection refused", ErrConnectionRefused, ErrConnectionRefused, true},
		{"different errors", ErrAliasNotFound, ErrAliasAlreadyExists, false},
		{"different timeout vs not found", ErrTimeout, ErrAliasNotFound, false},
		{"different connection vs timeout", ErrConnectionRefused, ErrTimeout, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.err1.Is(tc.err2)
			assert.Equal(t, tc.same, result)
		})
	}
}

func TestErrorsPackage_Integration_WithErrorType(t *testing.T) {
	baseErr := ErrTimeout

	// Test fmt.Errorf with %w
	wrappedErr := fmt.Errorf("operation failed: %w", baseErr)

	// Test errors.Is
	assert.True(t, errors.Is(wrappedErr, ErrTimeout))
	assert.False(t, errors.Is(wrappedErr, ErrAliasNotFound))

	// Test errors.As
	var target ErrorType
	assert.True(t, errors.As(wrappedErr, &target))
	assert.Equal(t, ErrTimeout, target)

	// Test errors.Unwrap
	unwrapped := errors.Unwrap(wrappedErr)
	assert.Equal(t, ErrTimeout, unwrapped)
}

func TestErrorsPackage_ChainedWrapping(t *testing.T) {
	// Create a wrapped error
	baseErr := ErrTimeout
	wrappedErr := fmt.Errorf("database query failed: %w", baseErr)

	// Wrap it again
	chainedErr := fmt.Errorf("database operation failed: %w", wrappedErr)

	// Test that errors.Is still works through the chain
	assert.True(t, errors.Is(chainedErr, ErrTimeout))
	assert.False(t, errors.Is(chainedErr, ErrAliasNotFound))
}

func TestErrorType_TypeAssertions(t *testing.T) {
	var err error = ErrAliasNotFound

	// Test type assertion
	errType, ok := err.(ErrorType)
	assert.True(t, ok)
	assert.Equal(t, ErrAliasNotFound, errType)

	// Test with errors.As
	var target ErrorType
	assert.True(t, errors.As(err, &target))
	assert.Equal(t, ErrAliasNotFound, target)
}

func TestErrorType_InterfaceCompatibility(t *testing.T) {
	// Test that ErrorType implements error interface
	var err error = ErrAliasNotFound
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "An entry matching the provided alias cannot be found.")
}

func TestErrorType_WrappingScenarios(t *testing.T) {
	// Test scenarios that demonstrate the new Is() method works with wrapped errors
	// This simulates what wrapError would produce

	baseErr := ErrAliasNotFound

	// Test simple wrapping
	wrappedErr := fmt.Errorf("get operation failed: %w", baseErr)
	assert.True(t, errors.Is(wrappedErr, ErrAliasNotFound))

	// Test with context-like wrapping
	contextErr := fmt.Errorf("connect (operation=connect, uri=localhost:2836): %w", baseErr)
	assert.True(t, errors.Is(contextErr, ErrAliasNotFound))

	// Test chained wrapping
	chainedErr := fmt.Errorf("database operation failed: %w", wrappedErr)
	assert.True(t, errors.Is(chainedErr, ErrAliasNotFound))
}

func TestErrorType_BackwardCompatibility(t *testing.T) {
	// Test that the existing error behavior is preserved
	// This simulates what makeErrorOrNil would return

	aliasNotFoundErr := ErrAliasNotFound

	// Test that the error still works as before
	assert.Error(t, aliasNotFoundErr)
	assert.Contains(t, aliasNotFoundErr.Error(), "An entry matching the provided alias cannot be found.")

	// Test that the new Is() method works
	assert.True(t, errors.Is(aliasNotFoundErr, ErrAliasNotFound))
	assert.False(t, errors.Is(aliasNotFoundErr, ErrAliasAlreadyExists))
}

// ------------------------------------------------------------------
// Additional comprehensive tests for error handling patterns
// ------------------------------------------------------------------

func TestErrorType_Is_AllKeyErrorTypes(t *testing.T) {
	// Test key error types that are commonly used
	keyErrors := []struct {
		name string
		err  ErrorType
	}{
		{"alias not found", ErrAliasNotFound},
		{"alias already exists", ErrAliasAlreadyExists},
		{"timeout", ErrTimeout},
		{"connection refused", ErrConnectionRefused},
		{"invalid argument", ErrInvalidArgument},
		{"access denied", ErrAccessDenied},
		{"not connected", ErrNotConnected},
		{"invalid query", ErrInvalidQuery},
	}

	for _, keyErr := range keyErrors {
		t.Run(keyErr.name+"_self_comparison", func(t *testing.T) {
			assert.True(t, keyErr.err.Is(keyErr.err))
		})

		t.Run(keyErr.name+"_with_errors_Is", func(t *testing.T) {
			assert.True(t, errors.Is(keyErr.err, keyErr.err))
		})

		t.Run(keyErr.name+"_wrapped_scenario", func(t *testing.T) {
			// Test wrapping scenarios similar to what wrapError would create
			wrappedErr := fmt.Errorf("operation failed: %w", keyErr.err)
			assert.True(t, errors.Is(wrappedErr, keyErr.err))

			// Test with context-like wrapping
			contextErr := fmt.Errorf("operation (key=value, timeout=5s): %w", keyErr.err)
			assert.True(t, errors.Is(contextErr, keyErr.err))
		})
	}
}

func TestErrorType_Is_NegativeTests(t *testing.T) {
	// Test that different errors are not equal to each other
	testPairs := []struct {
		name string
		err1 ErrorType
		err2 ErrorType
	}{
		{"alias not found vs alias exists", ErrAliasNotFound, ErrAliasAlreadyExists},
		{"timeout vs connection refused", ErrTimeout, ErrConnectionRefused},
		{"invalid argument vs access denied", ErrInvalidArgument, ErrAccessDenied},
		{"not connected vs invalid query", ErrNotConnected, ErrInvalidQuery},
	}

	for _, pair := range testPairs {
		t.Run(pair.name, func(t *testing.T) {
			assert.False(t, pair.err1.Is(pair.err2))
			assert.False(t, pair.err2.Is(pair.err1))
			assert.False(t, errors.Is(pair.err1, pair.err2))
			assert.False(t, errors.Is(pair.err2, pair.err1))
		})
	}
}

func TestErrorType_DeepWrappingScenarios(t *testing.T) {
	// Test deep wrapping scenarios that would occur with wrapError usage
	baseErr := ErrTimeout

	// Level 1: Basic wrapping (simulating wrapError)
	level1 := fmt.Errorf("query (table=stocks, timeout=30s): %w", baseErr)
	assert.True(t, errors.Is(level1, ErrTimeout))

	// Level 2: Service layer wrapping
	level2 := fmt.Errorf("database service failed: %w", level1)
	assert.True(t, errors.Is(level2, ErrTimeout))

	// Level 3: Application layer wrapping
	level3 := fmt.Errorf("API request failed: %w", level2)
	assert.True(t, errors.Is(level3, ErrTimeout))

	// Test errors.As works through deep wrapping
	var target ErrorType
	assert.True(t, errors.As(level3, &target))
	assert.Equal(t, ErrTimeout, target)
}

func TestErrorType_ErrorAsWithDifferentTypes(t *testing.T) {
	// Test errors.As with different error types
	baseErr := ErrAliasNotFound
	wrappedErr := fmt.Errorf("operation failed: %w", baseErr)

	// Test successful As
	var target ErrorType
	assert.True(t, errors.As(wrappedErr, &target))
	assert.Equal(t, ErrAliasNotFound, target)

	// Test As with wrong type
	var wrongTarget *ErrorType
	assert.False(t, errors.As(wrappedErr, &wrongTarget))

	// Test As with interface - remove this as errors.As doesn't work with *error
	// The ErrorType test above already validates the As functionality
}
