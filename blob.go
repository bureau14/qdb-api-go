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
func (handle HandleType) BlobPut(alias string, content string, expiry TimeType) error {
	e := C.qdb_blob_put(handle.t, C.CString(alias), unsafe.Pointer(C.CString(content)), C.qdb_size_t(len(content)), C.qdb_time_t(expiry))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// BlobPutSimple : simpler blob put
func (handle HandleType) BlobPutSimple(alias string, content string) error {
	ptr := unsafe.Pointer(C.CString(content))
	e := C.qdb_blob_put(handle.t, C.CString(alias), ptr, C.qdb_size_t(len(content)), NeverExpires)
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// BlobGet : blob get
func (handle HandleType) BlobGet(alias string) (string, error) {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	e := C.qdb_blob_get(handle.t, C.CString(alias), &content, &contentLength)
	if e != 0 {
		return "", ErrorType(e)
	}
	output := C.GoStringN((*C.char)(unsafe.Pointer(content)), C.int(contentLength))
	handle.Release(content)
	return output, nil
}

// BlobGetAndRemove : blob get and remove
func (handle HandleType) BlobGetAndRemove(alias string) (string, error) {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	e := C.qdb_blob_get_and_remove(handle.t, C.CString(alias), &content, &contentLength)
	if e != 0 {
		return "", ErrorType(e)
	}
	output := C.GoStringN((*C.char)(unsafe.Pointer(content)), C.int(contentLength))
	handle.Release(content)
	return output, nil
}
