package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
	"unsafe"
)

// TsBlobPoint : timestamped data
type TsBlobPoint struct {
	timestamp time.Time
	content   []byte
}

// NewTsBlobPoint : Create new timeseries blob point
func NewTsBlobPoint(timestamp time.Time, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
}

// Timestamp : return data point timestamp
func (t TsBlobPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsBlobPoint) Content() []byte {
	return t.content
}

// GetBlob : gets a blob in row
func (t *TsBulk) GetBlob() ([]byte, error) {
	var content unsafe.Pointer
	defer t.h.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_ts_row_get_blob(t.table, C.qdb_size_t(t.index), &content, &contentLength)

	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	t.index++

	return output, wrapError(err, "ts_bulk_get_blob")
}

// RowSetBlob : Set blob at specified index in current row
func (t *TsBatch) RowSetBlob(index int64, content []byte) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}

	return wrapError(C.qdb_ts_batch_row_set_blob(t.table, valueIndex, contentPtr, contentSize), "ts_batch_row_set_blob", "index", valueIndex, "content_size", len(content))
}

// RowSetBlobNoCopy : Set blob at specified index in current row without copying it
func (t *TsBatch) RowSetBlobNoCopy(index int64, content []byte) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}

	return wrapError(C.qdb_ts_batch_row_set_blob_no_copy(t.table, valueIndex, contentPtr, contentSize), "ts_batch_row_set_blob_no_copy", "index", valueIndex, "content_size", len(content))
}
