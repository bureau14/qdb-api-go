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

// TsDoublePoint : timestamped double data point
type TsDoublePoint struct {
	timestamp time.Time
	content   float64
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp time.Time, value float64) TsDoublePoint {
	return TsDoublePoint{timestamp, value}
}

// Timestamp : return data point timestamp
func (t TsDoublePoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsDoublePoint) Content() float64 {
	return t.content
}

// :: internals
func (t TsDoublePoint) toStructC() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{toQdbTimespec(t.timestamp), C.double(t.content)}
}

func TsDoublePointToStructG(t C.qdb_ts_double_point) TsDoublePoint {
	return TsDoublePoint{TimespecToStructG(t.timestamp), float64(t.value)}
}

func doublePointArrayToC(pts ...TsDoublePoint) *C.qdb_ts_double_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_double_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}

	return &points[0]
}

func doublePointArrayToSlice(points *C.qdb_ts_double_point, length int) []C.qdb_ts_double_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.

	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_double_point{})]C.qdb_ts_double_point)(unsafe.Pointer(points))[:length:length]
}

func doublePointArrayToGo(points *C.qdb_ts_double_point, pointsCount C.qdb_size_t) []TsDoublePoint {
	length := int(pointsCount)
	output := make([]TsDoublePoint, length)
	if length > 0 {
		slice := doublePointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsDoublePointToStructG(s)
		}
	}

	return output
}

// GetDouble : gets a double in row
func (t *TsBulk) GetDouble() (float64, error) {
	var content C.double
	err := C.qdb_ts_row_get_double(t.table, C.qdb_size_t(t.index), &content)
	t.index++

	return float64(content), makeErrorOrNil(err)
}

// RowSetDouble : Set double at specified index in current row
func (t *TsBatch) RowSetDouble(index int64, value float64) error {
	valueIndex := C.qdb_size_t(index)

	return wrapError(C.qdb_ts_batch_row_set_double(t.table, valueIndex, C.double(value)), "ts_batch_row_set_double", "index", valueIndex, "value", value)
}
