package qdb

/*
	#include <qdb/ts.h>
*/
import "C"
import (
	"time"
)

// TsRange : timeseries range with begin and end timestamp
type TsRange struct {
	begin  time.Time
	end    time.Time
}

// Begin : returns the start of the time range
func (t TsRange) Begin() time.Time {
	return t.begin
}

// End : returns the end of the time range
func (t TsRange) End() time.Time {
	return t.end
}

// NewRange : creates a time range
func NewRange(begin, end time.Time) TsRange {
	return TsRange{begin: begin, end: end}
}

// :: internals
func (t TsRange) toStructC() C.qdb_ts_range_t {
	r := C.qdb_ts_range_t{begin: toQdbTimespec(t.begin), end: toQdbTimespec(t.end)}
	return r
}

func (t C.qdb_ts_range_t) toStructG() TsRange {
	r := NewRange(t.begin.toStructG(), t.end.toStructG())
	return r
}

func rangeArrayToC(rs ...TsRange) *C.qdb_ts_range_t {
	if len(rs) == 0 {
		return nil
	}
	var ranges []C.qdb_ts_range_t
	for _, r := range rs {
		ranges = append(ranges, r.toStructC())
	}
	return &ranges[0]
}
