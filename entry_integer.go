package qdb

/*
	#include <qdb/integer.h>
	#include <stdlib.h>
*/
import "C"

import (
	"time"
)

// IntegerEntry : int data type
type IntegerEntry struct {
	Entry
}

// Put : Creates a new signed 64-bit integer.
//
//	Atomically creates an entry of the given alias and sets it to a cross-platform signed 64-bit integer.
//	If the entry already exists, the function returns an error.
//
//	You can specify an expiry time or use NeverExpires if you don’t want the entry to expire.
//	If you want to create or update an entry use Update.
//
//	The value will be correctly translated independently of the endianness of the client’s platform.
func (entry IntegerEntry) Put(content int64, expiry time.Time) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	err := C.qdb_int_put(entry.handle, alias, C.qdb_int_t(content), toQdbTime(expiry))
	return wrapError(err, "integer_put", "alias", entry.alias, "value", content, "expiry", expiry)
}

// Update : Creates or updates a signed 64-bit integer.
//
//	Atomically updates an entry of the given alias to the provided value.
//	If the entry doesn’t exist, it will be created.
//
//	You can specify an expiry time or use NeverExpires if you don’t want the entry to expire.
func (entry *IntegerEntry) Update(newContent int64, expiry time.Time) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	err := C.qdb_int_update(entry.handle, alias, C.qdb_int_t(newContent), toQdbTime(expiry))
	return wrapError(err, "integer_update", "alias", entry.alias, "value", newContent, "expiry", expiry)
}

// Get : Atomically retrieves the value of a signed 64-bit integer.
//
//	Atomically retrieves the value of an existing 64-bit integer.
func (entry IntegerEntry) Get() (int64, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var content C.qdb_int_t
	err := C.qdb_int_get(entry.handle, alias, &content)
	output := int64(content)
	return output, wrapError(err, "integer_get", "alias", entry.alias)
}

// Add : Atomically increases or decreases a signed 64-bit integer.
//
//	The specified entry will be atomically increased (or decreased) according to the given addend value:
//		To increase the value, specify a positive added
//		To decrease the value, specify a negative added
//
//	The function return the result of the operation.
//	The entry must already exist.
func (entry IntegerEntry) Add(added int64) (int64, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var result C.qdb_int_t
	err := C.qdb_int_add(entry.handle, alias, C.qdb_int_t(added), &result)
	output := int64(result)
	return output, wrapError(err, "integer_add", "alias", entry.alias, "addend", added)
}
