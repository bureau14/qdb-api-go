package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/client.h>
*/
import "C"
import "unsafe"

// Release : release previously allocated qdb resource
func (handle HandleType) Release(buffer unsafe.Pointer) {
	C.qdb_release(handle.t, buffer)
}
