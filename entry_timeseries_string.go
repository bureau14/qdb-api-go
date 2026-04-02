package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
	"unsafe"
)

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

func TsStringPointToStructG(t C.qdb_ts_string_point) TsStringPoint {
	return TsStringPoint{TimespecToStructG(t.timestamp), C.GoStringN(t.content, C.int(t.content_length))}
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

// RowSetString : Set string at specified index in current row
func (t *TsBatch) RowSetString(index int64, content string) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)

	return wrapError(C.qdb_ts_batch_row_set_string(t.table, valueIndex, contentPtr, contentSize), "ts_batch_row_set_string", "index", valueIndex, "value_size", len(content))
}

// RowSetStringNoCopy : Set string at specified index in current row without copying it
func (t *TsBatch) RowSetStringNoCopy(index int64, content string) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)

	return wrapError(C.qdb_ts_batch_row_set_string_no_copy(t.table, valueIndex, contentPtr, contentSize), "ts_batch_row_set_string_no_copy", "index", valueIndex, "value_size", len(content))
}
