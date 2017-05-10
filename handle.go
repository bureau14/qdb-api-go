package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/client.h>
*/
import "C"

// HandleType obfuscating qdb_handle_t
type HandleType struct {
	t C.qdb_handle_t
}

// Close : open a tcp handle
func (h HandleType) Close() error {
	e := C.qdb_close(h.t)
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// Connect : connect a previously opened handle
func (h HandleType) Connect(clusterURI string) error {
	e := C.qdb_connect(h.t, C.CString(clusterURI))
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// Open : open a tcp handle
func (h *HandleType) Open() error {
	e := C.qdb_open((*C.qdb_handle_t)(&h.t), C.qdb_p_tcp)
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}
