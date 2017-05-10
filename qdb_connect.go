package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// connect : connect a previously opened handle
func connect(handle C.qdb_handle_t, clusterURI *C.char) C.qdb_error_t {
	return C.qdb_connect(handle, clusterURI)
}
