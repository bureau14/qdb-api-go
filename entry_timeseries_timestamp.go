package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"time"
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
