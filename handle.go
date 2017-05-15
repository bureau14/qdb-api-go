package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/client.h>
*/
import "C"

// HandleType obfuscating qdb_handle_t
type HandleType struct {
	handle C.qdb_handle_t
	uri    string
}

// Close : open a tcp handle
func (h HandleType) Close() error {
	e := C.qdb_close(h.handle)
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// Connect : connect a previously opened handle
func (h HandleType) Connect(clusterURI string) error {
	e := C.qdb_connect(h.handle, C.CString(clusterURI))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// NewHandle : Create a new handle, return error if needed
func NewHandle() (HandleType, error) {
	var h HandleType
	e := C.qdb_open((*C.qdb_handle_t)(&h.handle), C.qdb_p_tcp)
	if e != 0 {
		return h, ErrorType(e)
	}
	return h, nil
}

// Entries creators

// Blob : create a blob entry object
func (h HandleType) Blob(alias string) BlobEntry {
	return BlobEntry{entry{h, alias}}
}

// Integer : create an integer entry object
func (h HandleType) Integer(alias string) IntegerEntry {
	return IntegerEntry{entry{h, alias}}
}

// Timeserie : create an time entry object
func (h HandleType) Timeserie(alias string, columns []TsColumnInfo) TimeseriesEntry {
	return TimeseriesEntry{entry{h, alias}, columns}
}
