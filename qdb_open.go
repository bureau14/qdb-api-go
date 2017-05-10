package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// open : open a tcp handle
func open(handle C.qdb_handle_t) C.qdb_error_t {
	return C.qdb_open(&handle, C.qdb_p_tcp)
}
