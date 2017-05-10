package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// Connect : connect a previously opened handle
func Connect(handle HandleType, clusterURI string) error {
	e := C.qdb_connect(C.qdb_handle_t(handle), C.CString(clusterURI))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}
