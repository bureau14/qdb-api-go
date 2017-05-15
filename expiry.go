package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/client.h>
*/
import "C"

// Expiry : expiration value
type Expiry TimeType

// NeverExpires : constant value for unexpirable data
const NeverExpires = C.qdb_never_expires

// PreserverExpiration : constant value for preservation of expiration value
const PreserverExpiration = C.qdb_preserve_expiration
