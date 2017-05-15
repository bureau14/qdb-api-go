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

// BlobEntry : blob data type
type BlobEntry struct {
	entry
}

// Put : blob put value for alias
func (entry BlobEntry) Put(content []byte, expiry Expiry) error {
	alias := C.CString(entry.alias)
	contentPtr := unsafe.Pointer(&content[0])
	contentSize := C.qdb_size_t(len(content))
	e := C.qdb_blob_put(entry.handle, alias, contentPtr, contentSize, C.qdb_time_t(expiry))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// Update : blob update value of alias
func (entry *BlobEntry) Update(newContent []byte, newExpiry Expiry) error {
	alias := C.CString(entry.alias)
	newContentPtr := unsafe.Pointer(&newContent[0])
	contentSize := C.qdb_size_t(len(newContent))
	e := C.qdb_blob_update(entry.handle, alias, newContentPtr, contentSize, C.qdb_time_t(newExpiry))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// Get : blob get value associated with alias
func (entry BlobEntry) Get() ([]byte, error) {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	e := C.qdb_blob_get(entry.handle, C.CString(entry.alias), &content, &contentLength)
	if e != 0 {
		return nil, ErrorType(e)
	}
	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	entry.Release(content)
	return output, nil
}

// GetAndRemove : blob get and remove
func (entry BlobEntry) GetAndRemove() ([]byte, error) {
	var content unsafe.Pointer
	var contentLength C.qdb_size_t
	e := C.qdb_blob_get_and_remove(entry.handle, C.CString(entry.alias), &content, &contentLength)
	if e != 0 {
		return nil, ErrorType(e)
	}
	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	entry.Release(content)
	return output, nil
}
