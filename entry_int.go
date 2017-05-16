package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/integer.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"

// IntegerEntry : int data type
type IntegerEntry struct {
	entry
}

// Put : integer put value for alias
func (entry IntegerEntry) Put(content int64, expiry Expiry) error {
	alias := C.CString(entry.alias)
	err := C.qdb_int_put(entry.handle, alias, C.qdb_int_t(content), C.qdb_time_t(expiry))
	return makeErrorOrNil(err)
}

// Update : integer update value of alias
func (entry *IntegerEntry) Update(newContent int64, newExpiry Expiry) error {
	alias := C.CString(entry.alias)
	err := C.qdb_int_update(entry.handle, alias, C.qdb_int_t(newContent), C.qdb_time_t(newExpiry))
	return makeErrorOrNil(err)
}

// Get : integer get value associated with alias
func (entry IntegerEntry) Get() (int64, error) {
	var content C.qdb_int_t
	err := C.qdb_int_get(entry.handle, C.CString(entry.alias), &content)
	output := int64(content)
	return output, makeErrorOrNil(err)
}

// Add : integer add to value associated with alias
func (entry IntegerEntry) Add(added int64) (int64, error) {
	var result C.qdb_int_t
	err := C.qdb_int_add(entry.handle, C.CString(entry.alias), C.qdb_int_t(added), &result)
	output := int64(result)
	return output, makeErrorOrNil(err)
}
