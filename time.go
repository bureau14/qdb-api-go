package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
)

// Alias for a C.qdb_timespec_t so it can be used as `qdb.Timespec` by API users
type Timespec C.qdb_timespec_t

// Alias for `C.qdb_time_t` so it can be used as `qdb.Time` by API users
type Time C.qdb_time_t

// toQdbTimespec converts a Go time.Time to the C qdb_timespec_t representation.
func toQdbTimespec(tp time.Time) C.qdb_timespec_t {
	return C.qdb_timespec_t{C.qdb_time_t(tp.Unix()), C.qdb_time_t(tp.Nanosecond())}
}

// toQdbTime converts a time.Time to the qdb_time_t millisecond format used by the C API.
func toQdbTime(tp time.Time) C.qdb_time_t {
	if tp.Equal(PreserveExpiration()) {
		return C.qdb_preserve_expiration
	}

	return C.qdb_time_t(tp.UnixNano() / int64(time.Millisecond))
}

// toQdbRange builds a qdb_ts_range_t from begin and end times.
func toQdbRange(begin, end time.Time) C.qdb_ts_range_t {
	var ret C.qdb_ts_range_t
	ret.begin = toQdbTimespec(begin)
	ret.end = toQdbTimespec(end)

	return ret
}

// TimespecToStructG converts qdb_timespec_t to time.Time in local timezone.
func TimespecToStructG(tp C.qdb_timespec_t) time.Time {
	return time.Unix(int64(tp.tv_sec), int64(tp.tv_nsec))
}

// TimeToQdbTimespec writes t into out using the qdb_timespec_t format.
func TimeToQdbTimespec(t time.Time, out *C.qdb_timespec_t) {
	out.tv_nsec = C.qdb_time_t(t.Nanosecond())
	out.tv_sec = C.qdb_time_t(t.Unix())
}

// QdbTimespecToTime converts qdb_timespec_t to a UTC time.Time.
func QdbTimespecToTime(t C.qdb_timespec_t) time.Time {
	return time.Unix(int64(t.tv_sec), int64(t.tv_nsec)).UTC()
}

// TimeSliceToQdbTimespec converts a slice of time.Time values to qdb_timespec_t slice.
func TimeSliceToQdbTimespec(xs []time.Time) []C.qdb_timespec_t {
	ret := make([]C.qdb_timespec_t, len(xs))
	for i, x := range xs {
		TimeToQdbTimespec(x, &ret[i])
	}

	return ret
}

// QdbTimespecSliceToTime converts a slice of qdb_timespec_t to time.Time values.
func QdbTimespecSliceToTime(xs []C.qdb_timespec_t) []time.Time {
	ret := make([]time.Time, len(xs))

	for i, x := range xs {
		ret[i] = QdbTimespecToTime(x)
	}

	return ret
}
