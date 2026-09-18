package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
	#include <limits.h>

	qdb_int_t undefined_int64 = qdb_int64_undefined;
	qdb_size_t undefined_count = qdb_count_undefined;
*/
import "C"

import (
	"math"
	"time"
)

// Seconds and nanoseconds of NullTime.
const (
	nullTimeSec  = math.MinInt64/1_000_000_000 - 1
	nullTimeNsec = math.MinInt64 - nullTimeSec*1_000_000_000
)

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

// NullTime : return the time value that represents a null timestamp, math.MinInt64 nanoseconds since epoch
func NullTime() time.Time {
	return time.Unix(nullTimeSec, nullTimeNsec).UTC()
}

// IsNullTime : return true when t is the null timestamp
func IsNullTime(t time.Time) bool {
	return t.Unix() == nullTimeSec && t.Nanosecond() == nullTimeNsec
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
