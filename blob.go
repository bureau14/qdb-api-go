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

// BlobPut : blob put value for alias
func (handle HandleType) BlobPut(alias string, content string, expiry TimeType) error {
	e := C.qdb_blob_put(handle.t, C.CString(alias), unsafe.Pointer(C.CString(content)), C.qdb_size_t(len(content)), C.qdb_time_t(expiry))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// BlobUpdate : blob update value of alias
func (handle HandleType) BlobUpdate(alias string, newContent string, expiry TimeType) error {
	e := C.qdb_blob_update(handle.t, C.CString(alias), unsafe.Pointer(C.CString(newContent)), C.qdb_size_t(len(newContent)), C.qdb_time_t(expiry))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// BlobGet : blob get value associated with alias
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

// BlobRemove : blob remove value of alias
func (handle HandleType) BlobRemove(alias string) error {
	e := C.qdb_remove(handle.t, C.CString(alias))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// :: Less Usefull ::

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
