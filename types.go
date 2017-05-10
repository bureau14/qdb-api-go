package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// HandleType obfuscating qdb_handle_t
type HandleType C.qdb_handle_t

// ErrorType obfuscating qdb_error_t
type ErrorType C.qdb_error_t

// SizeType obfuscating qdb_size_t
type SizeType C.qdb_size_t

// TimeType obfuscating qdb_time_t
type TimeType C.qdb_time_t
