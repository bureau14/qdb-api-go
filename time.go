package qdb

/*
	#include <qdb/client.h>
*/
import "C"
import (
	"time"
)

// Alias for a C.qdb_timespec_t so it can be used as `qdb.Timespec` by API users
type Timespec C.qdb_timespec_t

// Alias for `C.qdb_time_t` so it can be used as `qdb.Time` by API users
type Time C.qdb_time_t

func toQdbTimespec(tp time.Time) C.qdb_timespec_t {
	return C.qdb_timespec_t{C.qdb_time_t(tp.Unix()), C.qdb_time_t(tp.Nanosecond())}
}

func toQdbTime(tp time.Time) C.qdb_time_t {
	if tp.Equal(PreserveExpiration()) {
		return C.qdb_preserve_expiration
	}
	return C.qdb_time_t(tp.UnixNano() / int64(time.Millisecond))
}

func TimespecToStructG(tp C.qdb_timespec_t) time.Time {
	return time.Unix(int64(tp.tv_sec), int64(tp.tv_nsec))
}

// Converts a single time.Time value to a native C qdb_timespec_t value
func TimeToQdbTimespec(t time.Time) C.qdb_timespec_t {
	nsec := C.qdb_time_t(t.Nanosecond())
	sec := C.qdb_time_t(t.Unix())

	return C.qdb_timespec_t{sec, nsec}
}

// Converts a single native C qdb_timespec_t to a time.Time
func QdbTimespecToTime(t C.qdb_timespec_t) time.Time {
	return time.Unix(int64(t.tv_sec), int64(t.tv_nsec)).UTC()
}

// Converts a slice of `time.Time` values to a slice of native C qdb_timespec_t values
func TimeSliceToQdbTimespec(xs *[]time.Time) *[]C.qdb_timespec_t {
	ret := make([]C.qdb_timespec_t, len(*xs))

	for i, x := range *xs {
		ret[i] = TimeToQdbTimespec(x)
	}

	return &ret
}

// Converts a slice of `time.Time` values to a slice of native C qdb_timespec_t values
func QdbTimespecSliceToTime(xs *[]C.qdb_timespec_t) *[]time.Time {
	ret := make([]time.Time, len(*xs))

	for i, x := range *xs {
		ret[i] = QdbTimespecToTime(x)
	}

	return &ret
}
