package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// Close : open a tcp handle
func Close(handle HandleType) error {
	e := C.qdb_close(C.qdb_handle_t(handle))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}
