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

// Create : create a new timeseries
func (entry TimeseriesEntry) Create() error {
	alias := C.CString(entry.alias)
	columns := (*C.qdb_ts_column_info_t)(&entry.columns[0])
	columnsCount := C.qdb_size_t(len(entry.columns))
	err := C.qdb_ts_create(entry.handle, alias, columns, columnsCount)
	return makeErrorOrNil(err)
}

// TsDoublePoint : timestamped data
type TsDoublePoint struct {
	timestamp TimespecType
	content   float64
}

func (ts TsDoublePoint) toQdbDoublePoint() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{C.qdb_timespec_t(ts.timestamp), C.double(ts.content)}
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp TimespecType, value float64) TsDoublePoint {
	return TsDoublePoint{timestamp, value}
}

// InsertDouble : insert points in a time series
func (entry TimeseriesEntry) InsertDouble(column string, points []TsDoublePoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	contentCount := C.qdb_size_t(len(points))
	content := make([]C.qdb_ts_double_point, contentCount)
	for i := C.qdb_size_t(0); i < contentCount; i++ {
		content[i] = points[i].toQdbDoublePoint()
	}
	err := C.qdb_ts_double_insert(entry.handle, alias, columnName, &content[0], contentCount)
	return makeErrorOrNil(err)
}

// TsBlobPoint : timestamped data
type TsBlobPoint struct {
	timestamp TimespecType
	content   []byte
}

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (ts TsBlobPoint) toQdbBlobPoint() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(ts.content))
	data := unsafe.Pointer(C.CString(string(ts.content)))
	return C.qdb_ts_blob_point{C.qdb_timespec_t(ts.timestamp), data, dataSize}
}

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp TimespecType, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
}

// InsertBlob : insert points in a time series
func (entry TimeseriesEntry) InsertBlob(column string, points []TsBlobPoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	contentCount := C.qdb_size_t(len(points))
	content := make([]C.qdb_ts_blob_point, contentCount)
	for i := C.qdb_size_t(0); i < contentCount; i++ {
		content[i] = points[i].toQdbBlobPoint()
	}
	err := C.qdb_ts_blob_insert(entry.handle, alias, columnName, &content[0], contentCount)
	return makeErrorOrNil(err)
}

// TsRange : timeseries range with begin and end timestamp
type TsRange C.qdb_ts_range_t

// NewTsRange : Create new timeseries range
func NewTsRange(begin, end TimespecType) TsRange {
	return TsRange{C.qdb_timespec_t(begin), C.qdb_timespec_t(end)}
}

// GetDoubleRanges : get ranges of double data points
func (entry TimeseriesEntry) GetDoubleRanges(column string, ranges []TsRange) ([]TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRanges := (*C.qdb_ts_range_t)(unsafe.Pointer(&ranges[0]))
	qdbRangesCount := C.qdb_size_t(len(ranges))
	var qdbPoints *C.qdb_ts_double_point
	defer entry.Release(unsafe.Pointer(qdbPoints))
	var qdbPointsCount C.qdb_size_t
	err := C.qdb_ts_double_get_ranges(entry.handle, alias, columnName, qdbRanges, qdbRangesCount, &qdbPoints, &qdbPointsCount)

	length := int(qdbPointsCount)
	output := make([]TsDoublePoint, length)
	tmpslice := (*[1 << 30]C.qdb_ts_double_point)(unsafe.Pointer(qdbPoints))[:length:length]
	for i, s := range tmpslice {
		output[i] = TsDoublePoint{TimespecType(s.timestamp), float64(s.value)}
	}
	return output, makeErrorOrNil(err)
}

// GetBlobRanges : get ranges of blob data points
func (entry TimeseriesEntry) GetBlobRanges(column string, ranges []TsRange) ([]TsBlobPoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRanges := (*C.qdb_ts_range_t)(unsafe.Pointer(&ranges[0]))
	qdbRangesCount := C.qdb_size_t(len(ranges))
	var qdbPoints *C.qdb_ts_blob_point
	defer entry.Release(unsafe.Pointer(qdbPoints))
	var qdbPointsCount C.qdb_size_t
	err := C.qdb_ts_blob_get_ranges(entry.handle, alias, columnName, qdbRanges, qdbRangesCount, &qdbPoints, &qdbPointsCount)

	length := int(qdbPointsCount)
	output := make([]TsBlobPoint, length)
	tmpslice := (*[1 << 30]C.qdb_ts_blob_point)(unsafe.Pointer(qdbPoints))[:length:length]
	for i, s := range tmpslice {
		output[i] = TsBlobPoint{TimespecType(s.timestamp), C.GoBytes(s.content, C.int(s.content_length))}
	}
	return output, makeErrorOrNil(err)
}
