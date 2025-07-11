// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: high-performance time series database client
// Types: ErrorType, Handle, Entry, Cluster
// Ex: qdb.Connect(uri).GetBlob(alias) → data
package qdb

/*
	#include <qdb/error.h>
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

// Error codes: retryable errors default true except logic/constraint/permission failures
//
// Network/transient (retryable):
// - ErrTimeout: network timeout
// - ErrConnectionRefused/Reset: connection failed
// - ErrUnstableCluster: temporary cluster issue
// - ErrTryAgain: explicit retry request
// - ErrResourceLocked: concurrent access conflict
// - ErrNetworkError: generic network failure
//
// Logic/programming (non-retryable):
// - ErrInvalidArgument: bad parameter
// - ErrIncompatibleType: type mismatch
// - ErrInvalidQuery: malformed query
// - ErrBufferTooSmall: insufficient buffer
//
// Constraints (non-retryable):
// - ErrAliasAlreadyExists: duplicate key
// - ErrEntryTooLarge: size limit exceeded
// - ErrQuotaExceeded: storage quota reached
//
// Permissions (non-retryable):
// - ErrAccessDenied: insufficient privileges
// - ErrOperationNotPermitted: forbidden operation
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

// IsRetryable checks if error is transient/retryable.
// Args:
//
//	err: any error (wrapped or direct)
//
// Returns:
//
//	true: network/resource errors → retry
//	false: logic/permission errors → fail fast
//
// Example:
//
//	if IsRetryable(err) { time.Sleep(backoff); retry() }
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Extract ErrorType from wrapped errors - enables retry logic
	// to work with contextual errors from wrapError()
	var errorType ErrorType
	if !errors.As(err, &errorType) {
		return true // Unknown errors assumed retryable to avoid data loss
	}

	// Retry decision matrix - prevents infinite loops on permanent failures
	// while allowing recovery from transient issues
	switch errorType {
	// Success - no retry needed
	case Success, Created:
		return false

	// Logic errors - retrying won't fix bad code
	case ErrInvalidArgument, ErrInvalidHandle, ErrInvalidIterator, ErrInvalidVersion,
		ErrInvalidProtocol, ErrInvalidReply, ErrInvalidQuery, ErrInvalidRegex,
		ErrInvalidCryptoKey, ErrBufferTooSmall, ErrNotImplemented, ErrIteratorEnd:
		return false

	// Schema errors - retrying won't change schema
	case ErrIncompatibleType, ErrColumnNotFound, ErrQueryTooComplex:
		return false

	// Constraint violations - retrying won't resolve conflicts
	case ErrAliasAlreadyExists, ErrElementAlreadyExists, ErrTagAlreadySet,
		ErrOutOfBounds, ErrOverflow, ErrUnderflow, ErrEntryTooLarge,
		ErrAliasTooLong, ErrUnmatchedContent, ErrReservedAlias:
		return false

	// Auth failures - retrying won't fix credentials
	case ErrAccessDenied, ErrLoginFailed, ErrOperationNotPermitted, ErrUnknownUser:
		return false

	// Config errors - retrying won't enable features
	case ErrOperationDisabled:
		return false

	// State errors - retrying won't create missing data
	case ErrContainerEmpty, ErrContainerFull, ErrElementNotFound, ErrTagNotSet,
		ErrAliasNotFound, ErrHostNotFound:
		return false

	// Data integrity - retrying won't fix corruption
	case ErrDataCorruption:
		return false

	// Resource exhaustion - retrying won't free disk space
	case ErrNoSpaceLeft, ErrQuotaExceeded:
		return false

	// Transient errors - retry may succeed after condition clears
	case ErrUninitialized, ErrSkipped, ErrTimeout, ErrConnectionRefused, ErrConnectionReset,
		ErrUnstableCluster, ErrTryAgain, ErrConflict, ErrNotConnected, ErrResourceLocked,
		ErrSystemRemote, ErrSystemLocal, ErrInternalRemote, ErrInternalLocal,
		ErrNoMemoryRemote, ErrNoMemoryLocal, ErrTransactionPartialFailure, ErrClockSkew,
		ErrInterrupted, ErrNetworkInbufTooSmall, ErrNetworkError, ErrPartialFailure,
		ErrAsyncPipeFull:
		return true

	// Unknown errors assumed retryable - prevents data loss from new errors
	default:
		return true
	}
}
