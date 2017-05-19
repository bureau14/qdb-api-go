package qdb

/*
	#cgo LDFLAGS: -L. -lqdb_api
	#include <qdb/client.h>
*/
import "C"

// Expiry : expiration value
type Expiry TimeType

// NeverExpires : constant value for unexpirable data
const NeverExpires = C.qdb_never_expires

// PreserveExpiration : constant value for preservation of expiration value
const PreserveExpiration = C.qdb_preserve_expiration
