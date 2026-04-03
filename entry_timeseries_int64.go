package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
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
