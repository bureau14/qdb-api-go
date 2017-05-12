package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// SizeType typedef qdb_size_t
type SizeType C.qdb_size_t

// TimeType typedef qdb_time_t
type TimeType C.qdb_time_t
