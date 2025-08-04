package qdb

/*
	#include <qdb/blob.h>
	#include <string.h>
	#include <stdlib.h>
*/
import "C"

import (
	"time"
	"unsafe"
)

// BlobEntry : blob data type
type BlobEntry struct {
	Entry
}

// Get : Retrieve an entry's content
//
//	If the entry does not exist, the function will fail and return 'alias not found' error.
func (entry BlobEntry) Get() ([]byte, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var content unsafe.Pointer
	defer entry.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_blob_get(entry.handle, alias, &content, &contentLength)

	output := C.GoBytes(content, C.int(contentLength))

	return output, wrapError(err, "blob_get", "alias", entry.alias)
}

// GetAndRemove : Atomically gets an entry from the quasardb server and removes it.
//
//	If the entry does not exist, the function will fail and return 'alias not found' error.
func (entry BlobEntry) GetAndRemove() ([]byte, error) {
	var content unsafe.Pointer
	defer entry.Release(content)
	var contentLength C.qdb_size_t
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	err := C.qdb_blob_get_and_remove(entry.handle, alias, &content, &contentLength)

	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))

	return output, wrapError(err, "blob_get_and_remove", "alias", entry.alias)
}

// Put : Creates a new entry and sets its content to the provided blob.
//
//	If the entry already exists the function will fail and will return 'alias already exists' error.
//	You can specify an expiry or use NeverExpires if you don’t want the entry to expire.
func (entry BlobEntry) Put(content []byte, expiry time.Time) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}
	err := C.qdb_blob_put(entry.handle, alias, contentPtr, contentSize, toQdbTime(expiry))

	return wrapError(err, "blob_put", "alias", entry.alias, "size", len(content), "expiry", expiry)
}

// Update : Creates or updates an entry and sets its content to the provided blob.
//
//	If the entry already exists, the function will modify the entry.
//	You can specify an expiry or use NeverExpires if you don’t want the entry to expire.
func (entry *BlobEntry) Update(newContent []byte, expiry time.Time) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	contentSize := C.qdb_size_t(len(newContent))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&newContent[0])
	}
	err := C.qdb_blob_update(entry.handle, alias, contentPtr, contentSize, toQdbTime(expiry))

	return wrapError(err, "blob_update", "alias", entry.alias, "size", len(newContent), "expiry", expiry)
}

// GetAndUpdate : Atomically gets and updates (in this order) the entry on the quasardb server.
//
//	The entry must already exist.
func (entry *BlobEntry) GetAndUpdate(newContent []byte, expiry time.Time) ([]byte, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	contentSize := C.qdb_size_t(len(newContent))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&newContent[0])
	}
	var contentLength C.qdb_size_t
	var content unsafe.Pointer
	defer entry.Release(content)
	err := C.qdb_blob_get_and_update(entry.handle, alias, contentPtr, contentSize, toQdbTime(expiry), &content, &contentLength)
	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))

	return output, wrapError(err, "blob_get_and_update", "alias", entry.alias, "content_size", len(newContent))
}

// CompareAndSwap : Atomically compares the entry with comparand and updates it to new_value if, and only if, they match.
//
//	The function returns the original value of the entry in case of a mismatch. When it matches, no content is returned.
//	The entry must already exist.
//	Update will occur if and only if the content of the entry matches bit for bit the content of the comparand buffer.
func (entry *BlobEntry) CompareAndSwap(newValue, newComparand []byte, expiry time.Time) ([]byte, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	valueLength := C.qdb_size_t(len(newValue))
	value := unsafe.Pointer(nil)
	if valueLength != 0 {
		value = unsafe.Pointer(&newValue[0])
	}
	comparandLength := C.qdb_size_t(len(newComparand))
	comparand := unsafe.Pointer(nil)
	if comparandLength != 0 {
		comparand = unsafe.Pointer(&newComparand[0])
	}
	var originalLength C.qdb_size_t
	var originalValue unsafe.Pointer
	defer entry.Release(unsafe.Pointer(originalValue))
	err := C.qdb_blob_compare_and_swap(entry.handle, alias, value, valueLength, comparand, comparandLength, toQdbTime(expiry), &originalValue, &originalLength)
	output := C.GoBytes(originalValue, C.int(originalLength))

	return output, wrapError(err, "blob_compare_and_swap", "alias", entry.alias, "comparand_size", len(newComparand), "new_size", len(newValue), "expiry", expiry)
}

// RemoveIf : Atomically removes the entry on the server if the content matches.
//
//	The entry must already exist.
//	Removal will occur if and only if the content of the entry matches bit for bit the content of the comparand buffer.
func (entry BlobEntry) RemoveIf(comparand []byte) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	comparandLength := C.qdb_size_t(len(comparand))
	comparandC := unsafe.Pointer(nil)
	if comparandLength != 0 {
		comparandC = unsafe.Pointer(&comparand[0])
	}
	err := C.qdb_blob_remove_if(entry.handle, alias, comparandC, comparandLength)

	return wrapError(err, "blob_remove_if", "alias", entry.alias, "comparand_size", len(comparand))
}

// GetNoAlloc : Retrieve an entry's content to already allocated buffer
//
//	If the entry does not exist, the function will fail and return 'alias not found' error.
//	If the buffer is not large enough to hold the data, the function will fail
//	and return `buffer is too small`, content length will nevertheless be
//	returned with entry size so that the caller may resize its buffer and try again.
func (entry BlobEntry) GetNoAlloc(content []byte) (int, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	contentLength := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentLength != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}

	err := C.qdb_blob_get_noalloc(entry.handle, alias, contentPtr, &contentLength)

	return int(contentLength), wrapError(err, "blob_get_length", "alias", entry.alias)
}
