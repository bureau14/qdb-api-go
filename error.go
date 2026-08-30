// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: high-performance time series database client
// Types: ErrorType, Handle, Entry, Cluster
// Ex: qdb.Connect(uri).GetBlob(alias) → data
package qdb

/*
	#cgo noescape qdb_go_success
	#cgo nocallback qdb_go_success
	#cgo noescape qdb_go_error_origin
	#cgo nocallback qdb_go_error_origin
	#cgo noescape qdb_go_error_severity
	#cgo nocallback qdb_go_error_severity

	#include <qdb/error.h>

	// Wrappers for the qdb/error.h macros, which cgo cannot call directly.

	static inline int qdb_go_success(qdb_error_t e)
	{
		return QDB_SUCCESS(e);
	}

	static inline qdb_error_origin_t qdb_go_error_origin(qdb_error_t e)
	{
		return (qdb_error_origin_t)QDB_ERROR_ORIGIN(e);
	}

	static inline qdb_error_severity_t qdb_go_error_severity(qdb_error_t e)
	{
		return (qdb_error_severity_t)QDB_ERROR_SEVERITY(e);
	}
*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
)

// Error handling patterns for qdb-api-go:
//
// 1. Check retryability with exponential backoff:
//
//	err := handle.PutBlob(alias, data)
//	for attempt := 0; err != nil && IsRetryable(err) && attempt < 3; attempt++ {
//	    time.Sleep(time.Second * time.Duration(1<<attempt))
//	    err = handle.PutBlob(alias, data)
//	}
//
// 2. Use errors.Is() for specific error checks:
//
//	if errors.Is(err, qdb.ErrAliasNotFound) {
//	    // Create new entry
//	} else if errors.Is(err, qdb.ErrAccessDenied) {
//	    // Handle auth failure
//	}
//
// 3. Extract ErrorType from wrapped errors:
//
//	var qdbErr qdb.ErrorType
//	if errors.As(err, &qdbErr) {
//	    switch qdbErr {
//	    case qdb.ErrTimeout:
//	        // Handle timeout
//	    case qdb.ErrQuotaExceeded:
//	        // Handle quota
//	    }
//	}

// ErrorType: QuasarDB error codes, wraps C.qdb_error_t
type ErrorType C.qdb_error_t

// Error codes, wraps the qdb_error_t enum. See ErrorType.ErrorClass.
const (
	Success                      ErrorType = C.qdb_e_ok
	Created                      ErrorType = C.qdb_e_ok_created
	ErrUninitialized             ErrorType = C.qdb_e_uninitialized
	ErrAliasNotFound             ErrorType = C.qdb_e_alias_not_found
	ErrAliasAlreadyExists        ErrorType = C.qdb_e_alias_already_exists
	ErrOutOfBounds               ErrorType = C.qdb_e_out_of_bounds
	ErrSkipped                   ErrorType = C.qdb_e_skipped
	ErrIncompatibleType          ErrorType = C.qdb_e_incompatible_type
	ErrContainerEmpty            ErrorType = C.qdb_e_container_empty
	ErrContainerFull             ErrorType = C.qdb_e_container_full
	ErrElementNotFound           ErrorType = C.qdb_e_element_not_found
	ErrElementAlreadyExists      ErrorType = C.qdb_e_element_already_exists
	ErrOverflow                  ErrorType = C.qdb_e_overflow
	ErrUnderflow                 ErrorType = C.qdb_e_underflow
	ErrTagAlreadySet             ErrorType = C.qdb_e_tag_already_set
	ErrTagNotSet                 ErrorType = C.qdb_e_tag_not_set
	ErrTimeout                   ErrorType = C.qdb_e_timeout
	ErrConnectionRefused         ErrorType = C.qdb_e_connection_refused
	ErrConnectionReset           ErrorType = C.qdb_e_connection_reset
	ErrUnstableCluster           ErrorType = C.qdb_e_unstable_cluster
	ErrTryAgain                  ErrorType = C.qdb_e_try_again
	ErrConflict                  ErrorType = C.qdb_e_conflict
	ErrNotConnected              ErrorType = C.qdb_e_not_connected
	ErrResourceLocked            ErrorType = C.qdb_e_resource_locked
	ErrSystemRemote              ErrorType = C.qdb_e_system_remote
	ErrSystemLocal               ErrorType = C.qdb_e_system_local
	ErrInternalRemote            ErrorType = C.qdb_e_internal_remote
	ErrInternalLocal             ErrorType = C.qdb_e_internal_local
	ErrNoMemoryRemote            ErrorType = C.qdb_e_no_memory_remote
	ErrNoMemoryLocal             ErrorType = C.qdb_e_no_memory_local
	ErrInvalidProtocol           ErrorType = C.qdb_e_invalid_protocol
	ErrHostNotFound              ErrorType = C.qdb_e_host_not_found
	ErrBufferTooSmall            ErrorType = C.qdb_e_buffer_too_small
	ErrNotImplemented            ErrorType = C.qdb_e_not_implemented
	ErrInvalidVersion            ErrorType = C.qdb_e_invalid_version
	ErrInvalidArgument           ErrorType = C.qdb_e_invalid_argument
	ErrInvalidHandle             ErrorType = C.qdb_e_invalid_handle
	ErrReservedAlias             ErrorType = C.qdb_e_reserved_alias
	ErrUnmatchedContent          ErrorType = C.qdb_e_unmatched_content
	ErrInvalidIterator           ErrorType = C.qdb_e_invalid_iterator
	ErrEntryTooLarge             ErrorType = C.qdb_e_entry_too_large
	ErrTransactionPartialFailure ErrorType = C.qdb_e_transaction_partial_failure
	ErrOperationDisabled         ErrorType = C.qdb_e_operation_disabled
	ErrOperationNotPermitted     ErrorType = C.qdb_e_operation_not_permitted
	ErrIteratorEnd               ErrorType = C.qdb_e_iterator_end
	ErrInvalidReply              ErrorType = C.qdb_e_invalid_reply
	ErrOkCreated                 ErrorType = C.qdb_e_ok_created
	ErrNoSpaceLeft               ErrorType = C.qdb_e_no_space_left
	ErrQuotaExceeded             ErrorType = C.qdb_e_quota_exceeded
	ErrAliasTooLong              ErrorType = C.qdb_e_alias_too_long
	ErrClockSkew                 ErrorType = C.qdb_e_clock_skew
	ErrAccessDenied              ErrorType = C.qdb_e_access_denied
	ErrLoginFailed               ErrorType = C.qdb_e_login_failed
	ErrColumnNotFound            ErrorType = C.qdb_e_column_not_found
	ErrQueryTooComplex           ErrorType = C.qdb_e_query_too_complex
	ErrInvalidCryptoKey          ErrorType = C.qdb_e_invalid_crypto_key
	ErrInvalidQuery              ErrorType = C.qdb_e_invalid_query
	ErrInvalidRegex              ErrorType = C.qdb_e_invalid_regex
	ErrUnknownUser               ErrorType = C.qdb_e_unknown_user
	ErrInterrupted               ErrorType = C.qdb_e_interrupted
	ErrNetworkInbufTooSmall      ErrorType = C.qdb_e_network_inbuf_too_small
	ErrNetworkError              ErrorType = C.qdb_e_network_error
	ErrDataCorruption            ErrorType = C.qdb_e_data_corruption
	ErrPartialFailure            ErrorType = C.qdb_e_partial_failure
	ErrAsyncPipeFull             ErrorType = C.qdb_e_async_pipe_full
)

// ErrorOrigin: origin bits of an error code, wraps C.qdb_error_origin_t
type ErrorOrigin C.qdb_error_origin_t

const (
	ErrorOriginSystemRemote ErrorOrigin = C.qdb_e_origin_system_remote
	ErrorOriginSystemLocal  ErrorOrigin = C.qdb_e_origin_system_local
	ErrorOriginConnection   ErrorOrigin = C.qdb_e_origin_connection
	ErrorOriginInput        ErrorOrigin = C.qdb_e_origin_input
	ErrorOriginOperation    ErrorOrigin = C.qdb_e_origin_operation
	ErrorOriginProtocol     ErrorOrigin = C.qdb_e_origin_protocol
)

// ErrorSeverity: severity bits of an error code, wraps C.qdb_error_severity_t
type ErrorSeverity C.qdb_error_severity_t

const (
	ErrorSeverityUnrecoverable ErrorSeverity = C.qdb_e_severity_unrecoverable
	ErrorSeverityError         ErrorSeverity = C.qdb_e_severity_error
	ErrorSeverityWarning       ErrorSeverity = C.qdb_e_severity_warning
	ErrorSeverityInfo          ErrorSeverity = C.qdb_e_severity_info
)

// ErrorClass: retry classification of an error
type ErrorClass uint8

const (
	// ErrorClassNone: nil, success, or an informational status code
	ErrorClassNone ErrorClass = iota

	// ErrorClassRetryable: transient or unclassified
	ErrorClassRetryable

	// ErrorClassFatal: caused by the caller; retrying cannot succeed
	ErrorClassFatal
)

// String returns the class name
func (c ErrorClass) String() string {
	switch c {
	case ErrorClassNone:
		return "none"
	case ErrorClassRetryable:
		return "retryable"
	case ErrorClassFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// ErrorClassifier: errors that report their own ErrorClass. The outermost
// implementation in an error chain wins.
type ErrorClassifier interface {
	ErrorClass() ErrorClass
}

func (e ErrorType) Error() string { return C.GoString(C.qdb_error(C.qdb_error_t(e))) }

// Is enables errors.Is() comparison for wrapped errors.
// Returns:
//
//	true: target is same ErrorType
//	false: different type
//
// Example:
//
//	errors.Is(err, qdb.ErrTimeout) // → true if timeout
func (e ErrorType) Is(target error) bool {
	t, ok := target.(ErrorType)
	if ok {
		return e == t
	}

	return false
}

// Origin returns the origin bits (QDB_ERROR_ORIGIN)
func (e ErrorType) Origin() ErrorOrigin {
	return ErrorOrigin(C.qdb_go_error_origin(C.qdb_error_t(e)))
}

// Severity returns the severity bits (QDB_ERROR_SEVERITY)
func (e ErrorType) Severity() ErrorSeverity {
	return ErrorSeverity(C.qdb_go_error_severity(C.qdb_error_t(e)))
}

// ErrorClass classifies the code: QDB_SUCCESS codes are none, the listed
// codes are fatal, everything else is retryable.
//
//nolint:exhaustive // unlisted codes are retryable
func (e ErrorType) ErrorClass() ErrorClass {
	if e.isSuccess() {
		return ErrorClassNone
	}

	switch e {
	case ErrNotImplemented, ErrIncompatibleType, ErrUninitialized, ErrOutOfBounds,
		ErrInvalidQuery, ErrAliasNotFound, ErrAliasAlreadyExists, ErrInvalidArgument,
		ErrNetworkInbufTooSmall:
		return ErrorClassFatal

	default:
		return ErrorClassRetryable
	}
}

// isSuccess returns true when the C API considers the code a success or an
// informational status rather than a failure (QDB_SUCCESS).
func (e ErrorType) isSuccess() bool {
	return C.qdb_go_success(C.qdb_error_t(e)) != 0
}

func makeErrorOrNil(err C.qdb_error_t) error {
	if err != 0 && err != C.qdb_e_ok_created {
		return ErrorType(err)
	}

	return nil
}

// wrapError wraps C error with context
// In: err C.qdb_error_t, op string, kv pairs
// Out: error with context, nil if success
// Ex: wrapError(err, "connect", "uri", uri) → "connect (operation=connect, uri=qdb://host): timeout"
func wrapError(err C.qdb_error_t, operation string, keyValues ...any) error {
	if err == 0 || err == C.qdb_e_ok_created {
		return nil
	}

	// Panic on odd kv args - prevents subtle bugs from missing values
	if len(keyValues)%2 != 0 {
		panic(fmt.Sprintf("wrapError: odd number of key-value arguments provided (%d). Keys and values must be provided in pairs.", len(keyValues)))
	}

	baseErr := ErrorType(err)

	// Pre-allocate builder capacity to avoid reallocation
	// because error formatting is on hot path for failures
	var sb strings.Builder
	sb.Grow(len(operation) + len(keyValues)*20 + 10)

	sb.WriteString(operation)

	if len(keyValues) > 0 {
		sb.WriteString(" (operation=")
		sb.WriteString(operation)

		// Format context pairs - allows debugging failures with full context
		for i := 0; i < len(keyValues); i += 2 {
			sb.WriteString(", ")
			sb.WriteString(fmt.Sprintf("%v", keyValues[i]))
			sb.WriteString("=")
			sb.WriteString(fmt.Sprintf("%v", keyValues[i+1]))
		}

		sb.WriteString(")")
	}

	sb.WriteString(": ")

	return fmt.Errorf("%s%w", sb.String(), baseErr)
}

// ClassifyError returns whether err is a status signal, a retryable failure
// or a fatal one. Wrapped errors are unwrapped; the outermost
// ErrorClassifier in the chain decides.
//
// Returns:
//
	//	ErrorClassNone: err is nil, or the classifier reports none (success / informational status codes)
	//	ErrorClassRetryable: no ErrorClassifier in the chain
	//	otherwise: the classifier's ErrorClass()
func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ErrorClassNone
	}

	var classifier ErrorClassifier
	if errors.As(err, &classifier) {
		return classifier.ErrorClass()
	}

	return ErrorClassRetryable
}

// IsRetryable returns true when the error is retryable: the failure is
// transient, or at least not caused by the caller. nil, informational
// status codes and fatal errors are not retryable.
//
// Example:
//
//	if IsRetryable(err) { time.Sleep(backoff); retry() }
func IsRetryable(err error) bool {
	return ClassifyError(err) == ErrorClassRetryable
}

// IsFatal returns true when the error is caused by the caller and retrying
// the same call cannot succeed. nil and informational status codes are not
// fatal.
//
// Example:
//
//	if IsFatal(err) { return err }
func IsFatal(err error) bool {
	return ClassifyError(err) == ErrorClassFatal
}
