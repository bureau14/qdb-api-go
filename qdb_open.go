package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// Open : open a tcp handle
func Open(handle *HandleType) ErrorType {
	return ErrorType(C.qdb_open((*C.qdb_handle_t)(handle), C.qdb_p_tcp))
}
