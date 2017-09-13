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
	columns []TsColumnInfo
}

// ColumnInfos : return the current columns information
func (entry TimeseriesEntry) ColumnInfos() []TsColumnInfo {
	return entry.columns
}

// Create : create a new timeseries
func (entry TimeseriesEntry) Create() error {
	alias := C.CString(entry.alias)
	columnsCount := C.qdb_size_t(len(entry.columns))
	columns := columnInfoArrayToC(entry.columns...)
	err := C.qdb_ts_create(entry.handle, alias, columns, columnsCount)
	if err == 0 {
		entry.columns = columnInfoArrayToGo(columns, columnsCount)
	}
	return makeErrorOrNil(err)
}

// InsertDouble : Inserts double points in a time series.
//	Time series are distributed across the cluster and support efficient insertion anywhere within the time series as well as efficient lookup based on time.
//	If the time series does not exist, it will be created.
func (entry TimeseriesEntry) InsertDouble(column string, points ...TsDoublePoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	contentCount := C.qdb_size_t(len(points))
	content := doublePointArrayToC(points...)
	err := C.qdb_ts_double_insert(entry.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// InsertBlob : Inserts blob points in a time series.
//	Time series are distributed across the cluster and support efficient insertion anywhere within the time series as well as efficient lookup based on time.
//	If the time series does not exist, it will be created.
func (entry TimeseriesEntry) InsertBlob(column string, points []TsBlobPoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	contentCount := C.qdb_size_t(len(points))
	content := blobPointArrayToC(points...)
	err := C.qdb_ts_blob_insert(entry.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// GetDoubleRanges : Retrieves blobs in the specitypefied range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetDoubleRanges(column string, rgs ...TsRange) ([]TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_double_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_double_get_ranges(entry.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer entry.Release(unsafe.Pointer(points))
		return doublePointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// GetBlobRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetBlobRanges(column string, rgs ...TsRange) ([]TsBlobPoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_blob_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_blob_get_ranges(entry.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer entry.Release(unsafe.Pointer(points))
		return blobPointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// DoubleAggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) DoubleAggregate(column string, aggs ...*TsDoubleAggregation) ([]TsDoubleAggregation, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	aggregations := doubleAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsDoubleAggregation
	err := C.qdb_ts_double_aggregate(entry.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = doubleAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}

// BlobAggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) BlobAggregate(column string, aggs ...*TsBlobAggregation) ([]TsBlobAggregation, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	aggregations := blobAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsBlobAggregation
	err := C.qdb_ts_blob_aggregate(entry.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = blobAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}
