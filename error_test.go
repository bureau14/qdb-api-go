package qdb

import (
	"context"
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

// ------------------------------------------------------------------
// ErrorClass / ClassifyError / IsRetryable / IsFatal tests
// ------------------------------------------------------------------

// TestErrorType_ErrorClass validates the classification policy itself:
// every code listed as fatal in ErrorType.ErrorClass, a sample of codes
// the C API marks as success via QDB_SUCCESS, and a sample of the rest.
// The retryable sample includes codes the C API rates as unrecoverable
// (ErrConnectionRefused) and codes the previous IsRetryable rejected
// (ErrAccessDenied), to pin down that neither influences the result.
func TestErrorType_ErrorClass(t *testing.T) {
	tests := []struct {
		code ErrorType
		want ErrorClass
	}{
		// fatal
		{ErrNotImplemented, ErrorClassFatal},
		{ErrIncompatibleType, ErrorClassFatal},
		{ErrUninitialized, ErrorClassFatal},
		{ErrOutOfBounds, ErrorClassFatal},
		{ErrInvalidQuery, ErrorClassFatal},
		{ErrAliasNotFound, ErrorClassFatal},
		{ErrAliasAlreadyExists, ErrorClassFatal},
		{ErrInvalidArgument, ErrorClassFatal},

		// QDB_SUCCESS
		{Success, ErrorClassNone},
		{Created, ErrorClassNone},
		{ErrIteratorEnd, ErrorClassNone},
		{ErrTagAlreadySet, ErrorClassNone},
		{ErrElementNotFound, ErrorClassNone},

		// retryable
		{ErrTimeout, ErrorClassRetryable},
		{ErrConnectionRefused, ErrorClassRetryable},
		{ErrTryAgain, ErrorClassRetryable},
		{ErrAccessDenied, ErrorClassRetryable},
		{ErrQuotaExceeded, ErrorClassRetryable},
		{ErrOperationNotPermitted, ErrorClassRetryable},
		{ErrDataCorruption, ErrorClassRetryable},
		{ErrSkipped, ErrorClassRetryable},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.want, tc.code.ErrorClass(), "%v", tc.code)
	}
}

// TestErrorType_ErrorClass_UnknownCodeIsRetryable validates the default
// branch of ErrorType.ErrorClass: a code this package does not list, such
// as one introduced by a newer C API, must be retryable and never fatal.
func TestErrorType_ErrorClass_UnknownCodeIsRetryable(t *testing.T) {
	unknown := ErrTimeout + 0x0F00 // unlisted code number

	assert.Equal(t, ErrorClassRetryable, unknown.ErrorClass())
}

// TestErrorType_OriginSeverity validates that Origin and Severity decode
// the bits the way qdb/error.h defines them, one case per origin and
// severity value. ErrorClass relies on the severity bits via QDB_SUCCESS.
func TestErrorType_OriginSeverity(t *testing.T) {
	tests := []struct {
		code     ErrorType
		origin   ErrorOrigin
		severity ErrorSeverity
	}{
		{ErrTimeout, ErrorOriginConnection, ErrorSeverityError},
		{ErrConnectionRefused, ErrorOriginConnection, ErrorSeverityUnrecoverable},
		{ErrUninitialized, ErrorOriginInput, ErrorSeverityUnrecoverable},
		{ErrAliasNotFound, ErrorOriginOperation, ErrorSeverityWarning},
		{ErrNotImplemented, ErrorOriginSystemRemote, ErrorSeverityUnrecoverable},
		{ErrNetworkInbufTooSmall, ErrorOriginSystemLocal, ErrorSeverityError},
		{ErrInvalidProtocol, ErrorOriginProtocol, ErrorSeverityUnrecoverable},
		{ErrIteratorEnd, ErrorOriginOperation, ErrorSeverityInfo},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.origin, tc.code.Origin(), "origin of %v", tc.code)
		assert.Equal(t, tc.severity, tc.code.Severity(), "severity of %v", tc.code)
	}
}

// TestErrorClass_String validates the names used in log output and in the
// failure messages of the other tests, including the fallback for values
// outside the defined constants.
func TestErrorClass_String(t *testing.T) {
	assert.Equal(t, "none", ErrorClassNone.String())
	assert.Equal(t, "retryable", ErrorClassRetryable.String())
	assert.Equal(t, "fatal", ErrorClassFatal.String())
	assert.Equal(t, "unknown", ErrorClass(42).String())
}

// fatalWrapper: ErrorClassifier that reports fatal regardless of the
// wrapped error, standing in for a caller-defined error type.
type fatalWrapper struct{ err error }

func (w fatalWrapper) Error() string          { return "fatal: " + w.err.Error() }
func (w fatalWrapper) Unwrap() error          { return w.err }
func (w fatalWrapper) ErrorClass() ErrorClass { return ErrorClassFatal }

// TestClassifyError_Propagation validates how ClassifyError walks an error
// chain. Every error this package returns is either a bare ErrorType or one
// wrapped by wrapError, and callers wrap those again, so the class must
// survive any depth of wrapping. It also validates the two edges of the
// chain: errors with no ErrorType at all, and callers overriding the class
// through ErrorClassifier.
func TestClassifyError_Propagation(t *testing.T) {
	assert.Equal(t, ErrorClassNone, ClassifyError(nil))

	// bare ErrorType, one per class
	assert.Equal(t, ErrorClassRetryable, ClassifyError(ErrTimeout))
	assert.Equal(t, ErrorClassFatal, ClassifyError(ErrInvalidQuery))
	assert.Equal(t, ErrorClassNone, ClassifyError(ErrIteratorEnd))

	// wrapped once, as wrapError does
	wrapped := fmt.Errorf("blob_put (operation=blob_put, alias=x): %w", ErrAliasAlreadyExists)
	assert.Equal(t, ErrorClassFatal, ClassifyError(wrapped))

	// wrapped again by the caller
	doubleWrapped := fmt.Errorf("sink write failed: %w", fmt.Errorf("push: %w", ErrUnstableCluster))
	assert.Equal(t, ErrorClassRetryable, ClassifyError(doubleWrapped))

	// no ErrorType anywhere in the chain: retryable by default
	assert.Equal(t, ErrorClassRetryable, ClassifyError(errors.New("something else")))

	// caller-defined ErrorClassifier overrides the wrapped ErrorType, while
	// errors.Is still reaches the ErrorType
	overridden := fmt.Errorf("outer: %w", fatalWrapper{err: ErrTimeout})
	assert.Equal(t, ErrorClassFatal, ClassifyError(overridden))
	assert.True(t, errors.Is(overridden, ErrTimeout))
}

// TestIsRetryable_IsFatal validates the two boolean views over
// ClassifyError that callers actually use. The two are not complements:
// nil and informational codes are neither retryable nor fatal, which is
// what makes "!IsRetryable(err)" safe in a retry loop that already checked
// err != nil.
func TestIsRetryable_IsFatal(t *testing.T) {
	assert.False(t, IsRetryable(nil))
	assert.False(t, IsFatal(nil))

	// informational: neither
	assert.False(t, IsRetryable(ErrTagAlreadySet))
	assert.False(t, IsFatal(ErrTagAlreadySet))

	assert.False(t, IsRetryable(ErrIncompatibleType))
	assert.True(t, IsFatal(ErrIncompatibleType))

	// a reply larger than the client in-buffer: the caller sized the buffer,
	// so a reconnect cannot help
	assert.False(t, IsRetryable(ErrNetworkInbufTooSmall))
	assert.True(t, IsFatal(ErrNetworkInbufTooSmall))

	for _, code := range []ErrorType{ErrTimeout, ErrAccessDenied, ErrQuotaExceeded, ErrOperationNotPermitted} {
		assert.True(t, IsRetryable(code), "%v", code)
		assert.False(t, IsFatal(code), "%v", code)
	}

	// the error QueryPoint getters return on a type mismatch (query.go);
	// covers that change without needing a server
	getterErr := fmt.Errorf("query_point_get_double (operation=query_point_get_double, wrong_type=expected_double): %w", ErrIncompatibleType)
	assert.True(t, IsFatal(getterErr))
	assert.True(t, errors.Is(getterErr, ErrIncompatibleType))
}

// ------------------------------------------------------------------
// IsBadSession / IsClusterUnavailable tests
// ------------------------------------------------------------------

// predicateCase is one row of a predicate table: the error and the
// answer the predicate must give for it.
type predicateCase struct {
	err  error
	want bool
}

// checkPredicate runs pred over cases, failing on each mismatch with the
// predicate's name and the input.
func checkPredicate(t *testing.T, name string, pred func(error) bool, cases []predicateCase) {
	t.Helper()
	for _, tc := range cases {
		assert.Equal(t, tc.want, pred(tc.err), "%s(%v)", name, tc.err)
	}
}

// yesCases builds one true row per code.
func yesCases(codes ...ErrorType) []predicateCase {
	cases := make([]predicateCase, 0, len(codes))
	for _, c := range codes {
		cases = append(cases, predicateCase{c, true})
	}

	return cases
}

// noCases builds one false row per code.
func noCases(codes ...ErrorType) []predicateCase {
	cases := make([]predicateCase, 0, len(codes))
	for _, c := range codes {
		cases = append(cases, predicateCase{c, false})
	}

	return cases
}

// foreignCases are the rows every predicate answers false to: nil, and
// errors without an ErrorType in their chain.
func foreignCases() []predicateCase {
	return []predicateCase{
		{nil, false},
		{errors.New("something else"), false},
		{context.DeadlineExceeded, false},
		{fmt.Errorf("outer: %w", context.Canceled), false},
	}
}

// TestIsBadSession validates the full bad-session list, one code from
// every group that answers false, a wrapped code, nil and non-C errors.
func TestIsBadSession(t *testing.T) {
	cases := yesCases(
		ErrTimeout, ErrConnectionRefused, ErrConnectionReset, ErrNotConnected, ErrHostNotFound, ErrNetworkError,
		ErrUnstableCluster,
		ErrInvalidProtocol, ErrInvalidVersion, ErrInvalidReply,
		ErrSystemLocal, ErrInternalLocal, ErrNoMemoryLocal, ErrNetworkInbufTooSmall,
		ErrSystemRemote, ErrInternalRemote, ErrNoMemoryRemote, ErrDataCorruption, ErrInterrupted,
		ErrInvalidHandle, ErrUninitialized,
	)
	cases = append(cases, noCases(
		Success, Created, ErrOkCreated, ErrIteratorEnd, // success and informational
		ErrInvalidArgument, ErrInvalidQuery, // judged, input side
		ErrAliasNotFound, ErrColumnNotFound, ErrAccessDenied, ErrResourceLocked, ErrConflict, // judged, operation side
		ErrTryAgain, ErrAsyncPipeFull, ErrNotImplemented, ErrQuotaExceeded, // answered with a condition
	)...)
	cases = append(cases,
		predicateCase{fmt.Errorf("outer: %w", ErrTimeout), true},
		predicateCase{fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", ErrColumnNotFound)), false},
	)
	cases = append(cases, foreignCases()...)

	checkPredicate(t, "IsBadSession", IsBadSession, cases)
}

// TestIsClusterUnavailable validates the full cluster-unavailable list,
// codes that are retryable or bad-session without being evidence about
// the cluster, a wrapped code, nil and non-C errors.
func TestIsClusterUnavailable(t *testing.T) {
	cases := yesCases(
		ErrTimeout, ErrConnectionRefused, ErrConnectionReset, ErrNotConnected, ErrHostNotFound,
		ErrNetworkError, ErrUnstableCluster, ErrTryAgain, ErrAsyncPipeFull, ErrNoMemoryRemote,
	)
	cases = append(cases, noCases(
		Success, ErrIteratorEnd,
		ErrResourceLocked, ErrConflict, ErrTransactionPartialFailure, ErrInterrupted, // retryable, not the cluster
		ErrInvalidProtocol, ErrInvalidHandle, ErrSystemLocal, ErrInternalRemote, // bad session, not the cluster
		ErrAccessDenied, ErrColumnNotFound, ErrInvalidQuery, ErrQuotaExceeded, ErrLoginFailed,
	)...)
	cases = append(cases, predicateCase{fmt.Errorf("outer: %w", ErrConnectionRefused), true})
	cases = append(cases, foreignCases()...)

	checkPredicate(t, "IsClusterUnavailable", IsClusterUnavailable, cases)
}
