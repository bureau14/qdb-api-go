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
import "unsafe"

// BlobPut : original blob put
func BlobPut(handle HandleType, alias string, content *C.void, contentLength SizeType, expiry TimeType) ErrorType {
	e := C.qdb_blob_put(C.qdb_handle_t(handle), C.CString(alias), content, C.qdb_size_t(contentLength), C.qdb_time_t(expiry))
	return ErrorType(e)
}

// BlobPutSimple : simple blob put
func BlobPutSimple(handle HandleType, alias string, content string) ErrorType {
	ptr := unsafe.Pointer(C.CString(content))
	e := C.qdb_blob_put(C.qdb_handle_t(handle), C.CString(alias), ptr, C.qdb_size_t(len(content)), C.qdb_never_expires)
	return ErrorType(e)
}
