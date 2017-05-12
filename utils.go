package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/client.h>
*/
import "C"
import "unsafe"

// NeverExpires define
const NeverExpires = C.qdb_never_expires

// Release : release previously allocated qdb resource
func (handle HandleType) Release(buffer unsafe.Pointer) {
	C.qdb_release(handle.t, buffer)
}
