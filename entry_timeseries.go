package qdb

/*
	#include <qdb/ts.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// TimeseriesEntry : timeseries double entry data type
type TimeseriesEntry struct {
	Entry
}

// Columns : return the current columns
func (entry TimeseriesEntry) Columns() ([]TsDoubleColumn, []TsBlobColumn, error) {
	alias := C.CString(entry.alias)
	var columns *C.qdb_ts_column_info_t
	var columnsCount C.qdb_size_t
	err := C.qdb_ts_list_columns(entry.handle, alias, &columns, &columnsCount)
	var doubleColumns []TsDoubleColumn
	var blobColumns []TsBlobColumn
	if err == 0 {
		doubleColumns, blobColumns = columnArrayToGo(entry, columns, columnsCount)
	}
	return doubleColumns, blobColumns, makeErrorOrNil(err)
}

// ColumnsInfo : return the current columns information
func (entry TimeseriesEntry) ColumnsInfo() ([]TsColumnInfo, error) {
	alias := C.CString(entry.alias)
	var columns *C.qdb_ts_column_info_t
	var columnsCount C.qdb_size_t
	err := C.qdb_ts_list_columns(entry.handle, alias, &columns, &columnsCount)
	var columnsInfo []TsColumnInfo
	if err == 0 {
		columnsInfo = columnInfoArrayToGo(columns, columnsCount)
	}
	return columnsInfo, makeErrorOrNil(err)
}

// Create : create a new timeseries
func (entry TimeseriesEntry) Create(cols ...TsColumnInfo) error {
	alias := C.CString(entry.alias)
	columns := columnInfoArrayToC(cols...)
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_create(entry.handle, alias, columns, columnsCount)
	return makeErrorOrNil(err)
}

// InsertColumns : insert columns in a existing timeseries
func (entry TimeseriesEntry) InsertColumns(cols ...TsColumnInfo) error {
	alias := C.CString(entry.alias)
	columns := columnInfoArrayToC(cols...)
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_insert_columns(entry.handle, alias, columns, columnsCount)
	return makeErrorOrNil(err)
}

// DoubleColumn : create a column object
func (entry TimeseriesEntry) DoubleColumn(columnName string) TsDoubleColumn {
	return TsDoubleColumn{tsColumn{TsColumnInfo{columnName, TsColumnDouble}, entry}}
}

// BlobColumn : create a column object
func (entry TimeseriesEntry) BlobColumn(columnName string) TsBlobColumn {
	return TsBlobColumn{tsColumn{TsColumnInfo{columnName, TsColumnBlob}, entry}}
}

// EraseRanges : erase all points in the specified ranges
func (column TsDoubleColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	erasedCount := C.qdb_uint_t(0)
	err := C.qdb_ts_erase_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &erasedCount)
	return uint64(erasedCount), makeErrorOrNil(err)
}

// EraseRanges : erase all points in the specified ranges
func (column TsBlobColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	erasedCount := C.qdb_uint_t(0)
	err := C.qdb_ts_erase_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &erasedCount)
	return uint64(erasedCount), makeErrorOrNil(err)
}

// Insert double points into a timeseries
func (column TsDoubleColumn) Insert(points ...TsDoublePoint) error {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	contentCount := C.qdb_size_t(len(points))
	content := doublePointArrayToC(points...)
	err := C.qdb_ts_double_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// Insert blob points into a timeseries
func (column TsBlobColumn) Insert(points ...TsBlobPoint) error {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	contentCount := C.qdb_size_t(len(points))
	content := blobPointArrayToC(points...)
	err := C.qdb_ts_blob_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// GetRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (column TsDoubleColumn) GetRanges(rgs ...TsRange) ([]TsDoublePoint, error) {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_double_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_double_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return doublePointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// GetRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (column TsBlobColumn) GetRanges(rgs ...TsRange) ([]TsBlobPoint, error) {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_blob_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_blob_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return blobPointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// Aggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
//	It is an error to call this function on a non existing time-series.
func (column TsDoubleColumn) Aggregate(aggs ...*TsDoubleAggregation) ([]TsDoubleAggregation, error) {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	aggregations := doubleAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsDoubleAggregation
	err := C.qdb_ts_double_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = doubleAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}

// Aggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (column TsBlobColumn) Aggregate(aggs ...*TsBlobAggregation) ([]TsBlobAggregation, error) {
	alias := C.CString(column.parent.alias)
	columnName := C.CString(column.name)
	aggregations := blobAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsBlobAggregation
	err := C.qdb_ts_blob_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = blobAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}

// Bulk : create a bulk object for the specified columns
//	If no columns are specified it gets the server side registered columns
func (entry TimeseriesEntry) Bulk(cols ...TsColumnInfo) (*TsBulk, error) {
	if len(cols) == 0 {
		var err error
		cols, err = entry.ColumnsInfo()
		if err != nil {
			return nil, err
		}
	}
	alias := C.CString(entry.alias)
	columns := columnInfoArrayToC(cols...)
	columnsCount := C.qdb_size_t(len(cols))
	bulk := &TsBulk{alias: entry.alias, handle: entry.handle, columns: cols}
	err := C.qdb_ts_local_table_init(entry.handle, alias, columns, columnsCount, &bulk.table)
	return bulk, makeErrorOrNil(err)
}

// Row : initialize a row append
func (t *TsBulk) Row(timestamp time.Time) *TsBulk {
	t.timestamp = timestamp
	t.index = 0
	return t
}

// Double : adds a double in row transaction
func (t *TsBulk) Double(value float64) *TsBulk {
	if t.err == nil {
		t.err = makeErrorOrNil(C.qdb_ts_row_set_double(t.table, C.qdb_size_t(t.index), C.double(value)))
	}
	t.index++
	return t
}

// Blob : adds a blob in row transaction
func (t *TsBulk) Blob(content []byte) *TsBulk {
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}
	if t.err == nil {
		t.err = makeErrorOrNil(C.qdb_ts_row_set_blob(t.table, C.qdb_size_t(t.index), contentPtr, contentSize))
	}
	t.index++
	return t
}

// Ignore : ignores this column in a row transaction
func (t *TsBulk) Ignore() *TsBulk {
	t.index++
	return t
}

// Append : Adds the append to the list to be pushed
func (t *TsBulk) Append() error {
	if t.err != nil {
		return t.err
	}
	rowIndex := C.qdb_size_t(0)
	timespec := toQdbTimespec(t.timestamp)
	err := C.qdb_ts_table_row_append(t.table, &timespec, &rowIndex)
	if err == 0 {
		t.rowCount = int(rowIndex) + 1
	}
	t.timestamp = time.Unix(0, 0)
	return makeErrorOrNil(err)
}

// Push : push the list of appended rows
func (t *TsBulk) Push() error {
	err := C.qdb_ts_push(t.table)
	return makeErrorOrNil(err)
}

// Reset : reset the list, reusing the same columns
func (t *TsBulk) Reset() error {
	alias := C.CString(t.alias)
	columns := columnInfoArrayToC(t.columns...)
	columnsCount := C.qdb_size_t(len(t.columns))
	err := C.qdb_ts_local_table_init(t.handle, alias, columns, columnsCount, &t.table)
	return makeErrorOrNil(err)
}
