package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// Close : open a tcp handle
func Close(handle HandleType) ErrorType {
	return ErrorType(C.qdb_close(C.qdb_handle_t(handle)))
}
