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

// TsInt64Point : timestamped int64 data point
type TsInt64Point struct {
	timestamp time.Time
	content   int64
}

// NewTsInt64Point : Create new timeseries int64 point
func NewTsInt64Point(timestamp time.Time, value int64) TsInt64Point {
	return TsInt64Point{timestamp, value}
}

// Timestamp : return data point timestamp
func (t TsInt64Point) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsInt64Point) Content() int64 {
	return t.content
}

// :: internals
func (t TsInt64Point) toStructC() C.qdb_ts_int64_point {
	return C.qdb_ts_int64_point{toQdbTimespec(t.timestamp), C.qdb_int_t(t.content)}
}

func TsInt64PointToStructG(t C.qdb_ts_int64_point) TsInt64Point {
	return TsInt64Point{TimespecToStructG(t.timestamp), int64(t.value)}
}

func int64PointArrayToC(pts ...TsInt64Point) *C.qdb_ts_int64_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_int64_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}

	return &points[0]
}

func int64PointArrayToSlice(points *C.qdb_ts_int64_point, length int) []C.qdb_ts_int64_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.

	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_int64_point{})]C.qdb_ts_int64_point)(unsafe.Pointer(points))[:length:length]
}

func int64PointArrayToGo(points *C.qdb_ts_int64_point, pointsCount C.qdb_size_t) []TsInt64Point {
	length := int(pointsCount)
	output := make([]TsInt64Point, length)
	if length > 0 {
		slice := int64PointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsInt64PointToStructG(s)
		}
	}

	return output
}

// GetInt64 : gets an int64 in row
func (t *TsBulk) GetInt64() (int64, error) {
	var content C.qdb_int_t
	err := C.qdb_ts_row_get_int64(t.table, C.qdb_size_t(t.index), &content)
	t.index++

	return int64(content), wrapError(err, "ts_bulk_get_int64")
}

// RowSetInt64 : Set int64 at specified index in current row
func (t *TsBatch) RowSetInt64(index, value int64) error {
	valueIndex := C.qdb_size_t(index)

	return wrapError(C.qdb_ts_batch_row_set_int64(t.table, valueIndex, C.qdb_int_t(value)), "ts_batch_row_set_int64", "index", valueIndex, "value", value)
}
