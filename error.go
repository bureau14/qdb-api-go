package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
*/
import "C"

// ErrorType obfuscating qdb_error_t
type ErrorType C.qdb_error_t

func (e ErrorType) Error() string { return C.GoString(C.qdb_error(C.qdb_error_t(e))) }

func makeErrorOrNil(err C.qdb_error_t) error {
	if err != 0 {
		return ErrorType(err)
	}
	return nil
}
