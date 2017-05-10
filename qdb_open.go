package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// Open : open a tcp handle
func Open() (HandleType, error) {
	var handle HandleType
	e := C.qdb_open((*C.qdb_handle_t)(&handle), C.qdb_p_tcp)
	if e != 0 {
		return handle, ErrorType(e)
	}
	return handle, nil
}
