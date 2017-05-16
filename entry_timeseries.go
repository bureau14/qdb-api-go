package qdb

/*
	// import libqdb
	#cgo LDFLAGS: -lqdb_api
	#include <qdb/ts.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"
import "unsafe"

// TsColumnInfo : column information in timeseries
type TsColumnInfo C.qdb_ts_column_info_t

// TsColumnType : column type in timeseries
type TsColumnType C.qdb_ts_column_type_t

const (
	// TsColumnUnitialized : column is unititialized
	TsColumnUnitialized = -1
	// TsColumnDouble : column is a double point
	TsColumnDouble = 0
	// TsColumnBlob : column is a blob point
	TsColumnBlob = 1
)

// NewTsColumnInfo : create a column info structure
func NewTsColumnInfo(columnName string, columnType TsColumnType) TsColumnInfo {
	// The [4]byte is some sort of padding necessary for Go : struct(char *, int, 4 byte of padding)
	return TsColumnInfo{C.CString(columnName), C.qdb_ts_column_type_t(columnType), [4]byte{}}
}

// TimeseriesEntry : timeseries double entry data type
type TimeseriesEntry struct {
	entry
	columns []TsColumnInfo
}

// TsDoublePoint : timestamped data
type TsDoublePoint C.qdb_ts_double_point

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp TimespecType, value float64) TsDoublePoint {
	return TsDoublePoint{C.qdb_timespec_t(timestamp), C.double(value)}
}

// InsertDouble : insert points in a time series
func (entry TimeseriesEntry) InsertDouble(column string, points []TsDoublePoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	count := C.qdb_size_t(len(points))
	content := (*C.qdb_ts_double_point)(unsafe.Pointer(&points[0]))
	e := C.qdb_ts_double_insert(entry.handle, alias, columnName, content, count)
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}

// TsBlobPoint : timestamped data
type TsBlobPoint C.qdb_ts_blob_point

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp TimespecType, value []byte) TsBlobPoint {
	contentPtr := unsafe.Pointer(&value[0])
	contentSize := C.qdb_size_t(len(value))

	return TsBlobPoint{C.qdb_timespec_t(timestamp), contentPtr, contentSize}
}

// InsertBlob : insert points in a time series
func (entry TimeseriesEntry) InsertBlob(column string, points []TsBlobPoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	count := C.qdb_size_t(len(points))
	content := (*C.qdb_ts_blob_point)(unsafe.Pointer(&points[0]))
	e := C.qdb_ts_blob_insert(entry.handle, alias, columnName, content, count)
	if e != 0 {
		return ErrorType(e)
	}
	return nil
}
