package qdb

/*
	#include <qdb/error.h>
*/
import "C"

// ErrorType obfuscating qdb_error_t
type ErrorType C.qdb_error_t

func (e ErrorType) Error() string { return C.GoString(C.qdb_error(C.qdb_error_t(e))) }

func makeErrorOrNil(err C.qdb_error_t) error {
	if err != 0 && err != C.qdb_e_ok_created {
		return ErrorType(err)
	}
	return nil
}
