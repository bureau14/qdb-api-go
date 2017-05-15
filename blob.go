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

// Blob : blob data type
type Blob struct {
	entry
	content []byte
}

// Content : Return content value
func (blob Blob) Content() []byte {
	return blob.content
}

// NewBlob : Create a new blob type
func NewBlob(handle HandleType, expiry Expiry, alias string, content []byte) Blob {
	return Blob{entry{handle, expiry, alias}, content}
}

// Put : blob put value for alias
func (blob Blob) Put() error {
	alias := C.CString(blob.alias)
	content := unsafe.Pointer(&blob.content[0])
	contentSize := C.qdb_size_t(len(blob.content))
	e := C.qdb_blob_put(blob.handle, alias, content, contentSize, C.qdb_time_t(blob.Expiry))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// Update : blob update value of alias
func (blob *Blob) Update(newContent []byte, newExpiry Expiry) error {
	alias := C.CString(blob.alias)
	content := unsafe.Pointer(&blob.content[0])
	contentSize := C.qdb_size_t(len(newContent))
	e := C.qdb_blob_update(blob.handle, alias, content, contentSize, C.qdb_time_t(blob.Expiry))
	if e != 0 {
		return ErrorType(e)
	}
	blob.content = newContent
	blob.Expiry = newExpiry
	return nil
}

// Get : blob get value associated with alias
func (blob Blob) Get() ([]byte, error) {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	e := C.qdb_blob_get(blob.handle, C.CString(blob.alias), &content, &contentLength)
	if e != 0 {
		return nil, ErrorType(e)
	}
	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	blob.Release(content)
	return output, nil
}

// Remove : blob remove value of alias
func (blob Blob) Remove() error {
	e := C.qdb_remove(blob.handle, C.CString(blob.alias))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// GetAndRemove : blob get and remove
func (blob Blob) GetAndRemove() (string, error) {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	e := C.qdb_blob_get_and_remove(blob.handle, C.CString(blob.alias), &content, &contentLength)
	if e != 0 {
		return "", ErrorType(e)
	}
	output := C.GoStringN((*C.char)(unsafe.Pointer(content)), C.int(contentLength))
	blob.Release(content)
	return output, nil
}
