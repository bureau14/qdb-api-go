package qdb

/*
	#include <qdb/client.h>
*/
import "C"

// SizeType typedef qdb_size_t
type SizeType C.qdb_size_t

// TimeType : A cross-platform type that represents a time value.
// MUST be 64-bit large. The API will probably not link otherwise.
type TimeType C.qdb_time_t

// TimespecType typedef C.qdb_timespec_t
type TimespecType struct {
	Second     TimeType
	NanoSecond TimeType
}

func (time TimespecType) toQdbTimespec() C.qdb_timespec_t {
	return C.qdb_timespec_t{C.qdb_time_t(time.Second), C.qdb_time_t(time.NanoSecond)}
}

func (time C.qdb_timespec_t) toTimeSpec() TimespecType {
	return TimespecType{TimeType(time.tv_sec), TimeType(time.tv_nsec)}
}

// Equals : check if two timestamp are identical
func (time TimespecType) Equals(rhs TimespecType) bool {
	return time.Second == rhs.Second && time.NanoSecond == rhs.NanoSecond
}
