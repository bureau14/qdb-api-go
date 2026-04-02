package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"math"
	"time"
	"unsafe"
)

// TsTimestampPoint : timestamped timestamp data point
type TsTimestampPoint struct {
	timestamp time.Time
	content   time.Time
}

// NewTsTimestampPoint : Create new timeseries timestamp point
func NewTsTimestampPoint(timestamp, value time.Time) TsTimestampPoint {
	return TsTimestampPoint{timestamp, value}
}

// Timestamp : return data point timestamp
func (t TsTimestampPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsTimestampPoint) Content() time.Time {
	return t.content
}

// :: internals
func (t TsTimestampPoint) toStructC() C.qdb_ts_timestamp_point {
	return C.qdb_ts_timestamp_point{toQdbTimespec(t.timestamp), toQdbTimespec(t.content)}
}

func TsTimestampPointToStructG(t C.qdb_ts_timestamp_point) TsTimestampPoint {
	return TsTimestampPoint{TimespecToStructG(t.timestamp), TimespecToStructG(t.value)}
}

func timestampPointArrayToC(pts ...TsTimestampPoint) *C.qdb_ts_timestamp_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_timestamp_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}

	return &points[0]
}

func timestampPointArrayToSlice(points *C.qdb_ts_timestamp_point, length int) []C.qdb_ts_timestamp_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.

	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_timestamp_point{})]C.qdb_ts_timestamp_point)(unsafe.Pointer(points))[:length:length]
}

func timestampPointArrayToGo(points *C.qdb_ts_timestamp_point, pointsCount C.qdb_size_t) []TsTimestampPoint {
	length := int(pointsCount)
	output := make([]TsTimestampPoint, length)
	if length > 0 {
		slice := timestampPointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsTimestampPointToStructG(s)
		}
	}

	return output
}

// GetTimestamp : gets a timestamp in row
func (t *TsBulk) GetTimestamp() (time.Time, error) {
	var content C.qdb_timespec_t
	err := C.qdb_ts_row_get_timestamp(t.table, C.qdb_size_t(t.index), &content)
	t.index++

	return TimespecToStructG(content), wrapError(err, "ts_bulk_get_timestamp")
}

// RowSetTimestamp : Add a timestamp to current row
func (t *TsBatch) RowSetTimestamp(index int64, value time.Time) error {
	valueIndex := C.qdb_size_t(index)
	cValue := toQdbTimespec(value)

	return wrapError(C.qdb_ts_batch_row_set_timestamp(t.table, valueIndex, &cValue), "ts_batch_row_set_timestamp", "index", valueIndex)
}
