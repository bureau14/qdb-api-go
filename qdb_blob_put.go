package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/blob.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
	#include <string.h>
*/
import "C"

// blobPUT
func blobPUT(handle C.qdb_handle_t, alias *C.char, content *C.void, contentLength C.qdb_size_t, expiry C.qdb_time_t) C.qdb_error_t {
	return C.qdb_blob_put(handle, alias, content, contentLength, expiry)
}
