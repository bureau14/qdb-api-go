package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
	#include <limits.h>

	qdb_int_t undefined_int64 = qdb_int64_undefined;
	qdb_size_t undefined_count = qdb_count_undefined;
*/
import "C"
import "time"

// NeverExpires : return a time value corresponding to quasardb never expires value
func NeverExpires() time.Time {
	val := C.qdb_timespec_t{C.qdb_never_expires, C.qdb_never_expires}

	return TimespecToStructG(val)
}

// PreserveExpiration : return a time value corresponding to quasardb preserve expiration value
func PreserveExpiration() time.Time {
	val := C.qdb_timespec_t{C.qdb_preserve_expiration, C.qdb_preserve_expiration}

	return TimespecToStructG(val)
}

// MinTimespec : return a time value corresponding to quasardb minimum timespec value
func MinTimespec() time.Time {
	val := C.qdb_timespec_t{C.qdb_min_time, C.qdb_min_time}

	return TimespecToStructG(val)
}

// MaxTimespec : return a time value corresponding to quasardb maximum timespec value
func MaxTimespec() time.Time {
	val := C.qdb_timespec_t{C.qdb_max_time, C.qdb_max_time}

	return TimespecToStructG(val)
}

// Int64Undefined : return a int64 value corresponding to quasardb undefined int64 value
func Int64Undefined() int64 {
	val := int64(C.undefined_int64)

	return val
}

// CountUndefined : return a uint64 value corresponding to quasardb undefined count value
func CountUndefined() uint64 {
	val := uint64(C.undefined_count)

	return val
}
