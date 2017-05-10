package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// SizeType obfuscating qdb_size_t
type SizeType C.qdb_size_t

// TimeType obfuscating qdb_time_t
type TimeType C.qdb_time_t
