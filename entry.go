package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/tag.h>
	#include <stdlib.h>
*/
import "C"

type entry struct {
	HandleType
	Expiry
	alias []byte
}
