package qdb

/*
	#include <qdb/client.h>
*/
import "C"
import (
	"time"
)

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
