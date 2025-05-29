package qdb

/*
	#include <qdb/error.h>
*/
import "C"

// ErrorType obfuscating qdb_error_t
type ErrorType C.qdb_error_t

// Success : Success.
// Created : Success. A new entry has been created.
// ErrUninitialized : Uninitialized error.
// ErrAliasNotFound : Entry alias/key was not found.
// ErrAliasAlreadyExists : Entry alias/key already exists.
// ErrOutOfBounds : Index out of bounds.
// ErrSkipped : Skipped operation. Used in batches and transactions.
// ErrIncompatibleType : Entry or column is incompatible with the operation.
// ErrContainerEmpty : Container is empty.
// ErrContainerFull : Container is full.
// ErrElementNotFound : Element was not found.
// ErrElementAlreadyExists : Element already exists.
// ErrOverflow : Arithmetic operation overflows.
// ErrUnderflow : Arithmetic operation underflows.
// ErrTagAlreadySet : Tag is already set.
// ErrTagNotSet : Tag is not set.
// ErrTimeout : Operation timed out.
// ErrConnectionRefused : Connection was refused.
// ErrConnectionReset : Connection was reset.
// ErrUnstableCluster : Cluster is unstable.
// ErrTryAgain : Please retry.
// ErrConflict : There is another ongoing conflicting operation.
// ErrNotConnected : Handle is not connected.
// ErrResourceLocked : Resource is locked.
// ErrSystemRemote : System error on remote node (server-side). Please check errno or GetLastError() for actual error.
// ErrSystemLocal : System error on local system (client-side). Please check errno or GetLastError() for actual error.
// ErrInternalRemote : Internal error on remote node (server-side).
// ErrInternalLocal : Internal error on local system (client-side).
// ErrNoMemoryRemote : No memory on remote node (server-side).
// ErrNoMemoryLocal : No memory on local system (client-side).
// ErrInvalidProtocol : Protocol is invalid.
// ErrHostNotFound : Host was not found.
// ErrBufferTooSmall : Buffer is too small.
// ErrNotImplemented : Operation is not implemented.
// ErrInvalidVersion : Version is invalid.
// ErrInvalidArgument : Argument is invalid.
// ErrInvalidHandle : Handle is invalid.
// ErrReservedAlias : Alias/key is reserved.
// ErrUnmatchedContent : Content did not match.
// ErrInvalidIterator : Iterator is invalid.
// ErrEntryTooLarge : Entry is too large.
// ErrTransactionPartialFailure : Transaction failed partially.
// ErrOperationDisabled : Operation has not been enabled in cluster configuration.
// ErrOperationNotPermitted : Operation is not permitted.
// ErrIteratorEnd :	Iterator reached the end.
// ErrInvalidReply : Cluster sent an invalid reply.
// ErrNoSpaceLeft : No more space on disk.
// ErrQuotaExceeded : Disk space quota has been reached.
// ErrAliasTooLong : Alias is too long.
// ErrClockSkew : Cluster nodes have important clock differences.
// ErrAccessDenied : Access is denied.
// ErrLoginFailed : Login failed.
// ErrColumnNotFound : Column was not found.
// ErrQueryTooComplex : Find is too complex.
// ErrInvalidCryptoKey : Security key is invalid.
// ErrInvalidQuery : Query is invalid.
// ErrInvalidRegex : Regular expression is invalid.
// ErrUnknownUser : Unknown user.
// ErrInterrupted : Operation has been interrupted.
// ErrNetworkInbufTooSmall : Network input buffer is too small to complete the operation.
// ErrNetworkError : Network error.
// ErrDataCorruption : Data corruption has been detected.
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

func makeErrorOrNil(err C.qdb_error_t) error {
	if err != 0 && err != C.qdb_e_ok_created {
		return ErrorType(err)
	}
	return nil
}
