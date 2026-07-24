package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
)

// TsDoubleColumn : a time series double column
type TsDoubleColumn struct {
	tsColumn
}

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

// GetDouble : gets a double in row
func (t *TsBulk) GetDouble() (float64, error) {
	var content C.double
	err := C.qdb_ts_row_get_double(t.table, C.qdb_size_t(t.index), &content)
	t.index++

	return float64(content), makeErrorOrNil(err)
}

