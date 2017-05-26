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

// Get : Retrieves an entry’s content from the quasardb server.
func (entry BlobEntry) Get() ([]byte, error) {
	var content unsafe.Pointer
	defer entry.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_blob_get(entry.handle, C.CString(entry.alias), &content, &contentLength)

	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	return output, makeErrorOrNil(err)
}

// GetAndRemove : Atomically gets an entry from the quasardb server and removes it.
func (entry BlobEntry) GetAndRemove() ([]byte, error) {
	var content unsafe.Pointer
	defer entry.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_blob_get_and_remove(entry.handle, C.CString(entry.alias), &content, &contentLength)

	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	return output, makeErrorOrNil(err)
}

// Put : Creates a new entry and sets its content to the provided blob.
func (entry BlobEntry) Put(content []byte, expiry Expiry) error {
	alias := C.CString(entry.alias)
	contentSize := C.qdb_size_t(len(content))
	var contentPtr unsafe.Pointer
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	} else {
		contentPtr = unsafe.Pointer(nil)
	}
	err := C.qdb_blob_put(entry.handle, alias, contentPtr, contentSize, C.qdb_time_t(expiry))
	return makeErrorOrNil(err)
}

// Update : Creates or updates an entry and sets its content to the provided blob.
func (entry *BlobEntry) Update(newContent []byte, newExpiry Expiry) error {
	alias := C.CString(entry.alias)
	contentSize := C.qdb_size_t(len(newContent))
	var contentPtr unsafe.Pointer
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&newContent[0])
	} else {
		contentPtr = unsafe.Pointer(nil)
	}
	err := C.qdb_blob_update(entry.handle, alias, contentPtr, contentSize, C.qdb_time_t(newExpiry))
	return makeErrorOrNil(err)
}

// GetAndUpdate : Atomically gets and updates (in this order) the entry on the quasardb server.
func (entry *BlobEntry) GetAndUpdate(newContent []byte, newExpiry Expiry) ([]byte, error) {
	contentSize := C.qdb_size_t(len(newContent))
	var contentPtr unsafe.Pointer
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&newContent[0])
	} else {
		contentPtr = unsafe.Pointer(nil)
	}
	var content unsafe.Pointer
	defer entry.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_blob_get_and_update(entry.handle, C.CString(entry.alias), contentPtr, contentSize, C.qdb_time_t(newExpiry), &content, &contentLength)
	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	return output, makeErrorOrNil(err)
}
