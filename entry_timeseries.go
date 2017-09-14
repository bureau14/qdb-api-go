package qdb

/*
	#include <qdb/ts.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"
import (
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
