package qdb

/*
	#include <qdb/direct.h>
	#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"time"
	"unsafe"
)

// DirectHandleType is an opaque handle needed for maintaining a direct
// connection to a node.
type DirectHandleType struct {
	handle C.qdb_direct_handle_t
}

// DirectEntry is a base type for composition. Similar to a regular entry
type DirectEntry struct {
	DirectHandleType
	alias string
}

// DirectBlobEntry is an Entry for a blob data type
type DirectBlobEntry struct {
	DirectEntry
}

// DirectIntegerEntry is an Entry for a int data type
type DirectIntegerEntry struct {
	DirectEntry
}

// DirectConnect opens a connection to a node for use with the direct API
//
// The returned direct handle must be freed with Close(). Releasing the
// handle has no impact on non-direct connections or other direct handles.
func (h HandleType) DirectConnect(nodeURI string) (DirectHandleType, error) {
	uri := convertToCharStar(nodeURI)
	defer releaseCharStar(uri)

	var directHandle DirectHandleType
	directHandle.handle = C.qdb_direct_connect(h.handle, uri)

	if directHandle.handle == nil {
		err := fmt.Errorf("Unable to connect to node with URI: %s", nodeURI)
		return directHandle, err
	}

	return directHandle, nil
}

// Close releases a direct connect previously opened with DirectConnect
func (h DirectHandleType) Close() error {
	C.qdb_direct_close(h.handle)
	// the C api currently doesn't return an error but we want to keep the
	// option open later
	return nil
}

// Release frees API allocated buffers
func (h DirectHandleType) Release(buffer unsafe.Pointer) {
	// TODO(Mike): we are casting qdb_direct_handle_t to qdb_direct_t for
	// release, but should probably have a seperate qdb_direct_release api call
	unsafeHandle := (C.qdb_handle_t)(unsafe.Pointer(h.handle))
	C.qdb_release(unsafeHandle, buffer)
}

// Blob creates a direct blob entry object
func (h DirectHandleType) Blob(alias string) DirectBlobEntry {
	return DirectBlobEntry{DirectEntry{h, alias}}
}

// Integer creates a direct integer entry object
func (h DirectHandleType) Integer(alias string) DirectIntegerEntry {
	return DirectIntegerEntry{DirectEntry{h, alias}}
}

// PrefixGet : Retrieves the list of all entries matching the provided prefix.
//	A prefix-based search will enable you to find all entries matching a provided prefix.
//	This function returns the list of aliases. It’s up to the user to query the content associated with every entry, if needed.
func (h DirectHandleType) PrefixGet(prefix string, limit int) ([]string, error) {
	cPrefix := convertToCharStar(prefix)
	defer releaseCharStar(cPrefix)
	var entryCount C.size_t
	var entries **C.char
	err := C.qdb_direct_prefix_get(h.handle, cPrefix, C.qdb_int_t(limit), &entries, &entryCount)

	if err == 0 {
		defer h.Release(unsafe.Pointer(entries))
		length := int(entryCount)
		output := make([]string, length)
		if length > 0 {
			tmpslice := charStarArrayToSlice(entries, length)
			for i, s := range tmpslice {
				output[i] = C.GoString(s)
			}
		}
		return output, nil
	}
	return []string{}, ErrorType(err)
}

// Alias returns an alias name
func (e DirectEntry) Alias() string {
	return e.alias
}

// Remove an entry from the local node's storage, regardless of its type.
//
// This function bypasses the clustering mechanism and accesses the node
// local storage. Entries in the local node storage are not accessible via
// the regular API and vice versa.
//
// The call is ACID, regardless of the type of the entry and a transaction
// will be created if need be.
func (e DirectEntry) Remove() error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	err := C.qdb_direct_remove(e.handle, alias)
	return makeErrorOrNil(err)
}

// Get returns an entry's contents
func (e DirectBlobEntry) Get() ([]byte, error) {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)

	var content unsafe.Pointer
	defer e.Release(content)

	var contentLength C.qdb_size_t
	err := C.qdb_direct_blob_get(e.handle, alias, &content, &contentLength)

	output := C.GoBytes(content, C.int(contentLength))
	return output, makeErrorOrNil(err)
}

// Put creates a new entry and sets its content to the provided blob
// This will return an error if the entry alias already exists
// You can specify an expiry or use NeverExpires if you don’t want the entry to expire.
func (e DirectBlobEntry) Put(content []byte, expiry time.Time) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}
	err := C.qdb_direct_blob_put(e.handle, alias, contentPtr, contentSize, toQdbTime(expiry))
	return makeErrorOrNil(err)
}

// Update creates or updates an entry and sets its content to the provided blob.
// If the entry already exists, the function will modify the entry.
// You can specify an expiry or use NeverExpires if you don’t want the entry to expire.
func (e *DirectBlobEntry) Update(newContent []byte, expiry time.Time) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	contentSize := C.qdb_size_t(len(newContent))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&newContent[0])
	}
	err := C.qdb_direct_blob_update(e.handle, alias, contentPtr, contentSize, toQdbTime(expiry))
	return makeErrorOrNil(err)
}

// Get returns the value of a signed 64-bit integer
func (e DirectIntegerEntry) Get() (int64, error) {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	var content C.qdb_int_t
	err := C.qdb_direct_int_get(e.handle, alias, &content)
	output := int64(content)
	return output, makeErrorOrNil(err)
}

// Put creates a new signed 64-bit integer.
//	Atomically creates an entry of the given alias and sets it to a cross-platform signed 64-bit integer.
//	If the entry already exists, the function returns an error.
//
//	You can specify an expiry time or use NeverExpires if you don’t want the entry to expire.
//	If you want to create or update an entry use Update.
//
//	The value will be correctly translated independently of the endianness of the client’s platform.
func (e DirectIntegerEntry) Put(content int64, expiry time.Time) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	err := C.qdb_direct_int_put(e.handle, alias, C.qdb_int_t(content), toQdbTime(expiry))
	return makeErrorOrNil(err)
}

// Update creates or updates a signed 64-bit integer.
//	Atomically updates an entry of the given alias to the provided value.
//	If the entry doesn’t exist, it will be created.
//
//	You can specify an expiry time or use NeverExpires if you don’t want the entry to expire.
func (e DirectIntegerEntry) Update(newContent int64, expiry time.Time) error {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	err := C.qdb_direct_int_update(e.handle, alias, C.qdb_int_t(newContent), toQdbTime(expiry))
	return makeErrorOrNil(err)
}

// Add : Atomically increases or decreases a signed 64-bit integer.
//	The specified entry will be atomically increased (or decreased) according to the given addend value:
//		To increase the value, specify a positive added
//		To decrease the value, specify a negative added
//
//	The function return the result of the operation.
//	The entry must already exist.
func (e DirectIntegerEntry) Add(added int64) (int64, error) {
	alias := convertToCharStar(e.alias)
	defer releaseCharStar(alias)
	var result C.qdb_int_t
	err := C.qdb_direct_int_add(e.handle, alias, C.qdb_int_t(added), &result)
	output := int64(result)
	return output, makeErrorOrNil(err)
}
