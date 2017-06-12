package qdb

/*
	#include <qdb/client.h>
*/
import "C"
import "time"

// SizeType typedef qdb_size_t
type SizeType C.qdb_size_t

func toQdbTimespec(time time.Time) C.qdb_timespec_t {
	return C.qdb_timespec_t{C.qdb_time_t(time.Unix()), C.qdb_time_t(time.Nanosecond())}
}

func (cval C.qdb_timespec_t) toStructG() time.Time {
	return time.Unix(int64(cval.tv_sec), int64(cval.tv_nsec))
}
