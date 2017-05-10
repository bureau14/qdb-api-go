package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
*/
import "C"

// func (e ErrorType) String() string { return C.GoString(C.qdb_error(C.qdb_error_t(e))) }
func (e ErrorType) Error() string { return C.GoString(C.qdb_error(C.qdb_error_t(e))) }
