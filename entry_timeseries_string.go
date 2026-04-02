package qdb

/*
	#include <qdb/ts.h>
	#include <stdlib.h>
*/
import "C"

import (
	"math"
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

// :: internals
func (t TsStringPoint) toStructC() C.qdb_ts_string_point {
	dataSize := C.qdb_size_t(len(t.content))
	data := convertToCharStar(string(t.content))

	return C.qdb_ts_string_point{toQdbTimespec(t.timestamp), data, dataSize}
}

func TsStringPointToStructG(t C.qdb_ts_string_point) TsStringPoint {
	return TsStringPoint{TimespecToStructG(t.timestamp), C.GoStringN(t.content, C.int(t.content_length))}
}

func stringPointArrayToC(pts ...TsStringPoint) *C.qdb_ts_string_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_string_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}

	return &points[0]
}

func releaseStringPointArray(points *C.qdb_ts_string_point, length int) {
	if length > 0 {
		slice := stringPointArrayToSlice(points, length)
		for _, s := range slice {
			C.free(unsafe.Pointer(s.content))
		}
	}
}

func stringPointArrayToSlice(points *C.qdb_ts_string_point, length int) []C.qdb_ts_string_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.

	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_string_point{})]C.qdb_ts_string_point)(unsafe.Pointer(points))[:length:length]
}

func stringPointArrayToGo(points *C.qdb_ts_string_point, pointsCount C.qdb_size_t) []TsStringPoint {
	length := int(pointsCount)
	output := make([]TsStringPoint, length)
	if length > 0 {
		slice := stringPointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsStringPointToStructG(s)
		}
	}

	return output
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
