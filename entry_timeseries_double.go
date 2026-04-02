package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
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
