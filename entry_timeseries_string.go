package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
	"unsafe"
)

// TsStringColumn : a time series string column
type TsStringColumn struct {
	tsColumn
}

// TsStringPoint : timestamped data
type TsStringPoint struct {
	timestamp time.Time
	content   string
}

// NewTsStringPoint : Create new timeseries string point
func NewTsStringPoint(timestamp time.Time, value string) TsStringPoint {
	return TsStringPoint{timestamp, value}
}

// Timestamp : return data point timestamp
func (t TsStringPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsStringPoint) Content() string {
	return t.content
}

// GetString : gets a string in row
func (t *TsBulk) GetString() (string, error) {
	var content *C.char
	defer t.h.Release(unsafe.Pointer(content))
	var contentLength C.qdb_size_t
	err := C.qdb_ts_row_get_string(t.table, C.qdb_size_t(t.index), &content, &contentLength)

	t.index++

	return C.GoStringN(content, C.int(contentLength)), wrapError(err, "ts_bulk_get_string")
}

