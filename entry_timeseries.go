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
		length := int(columnsCount)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
			for i, s := range tmpslice {
				entry.columns[i] = s.toStructG()
			}
		}
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
func (entry TimeseriesEntry) GetDoubleRanges(column string, ranges ...TsRange) ([]TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	qdbRanges := rangeArrayToC(ranges...)
	var qdbPoints *C.qdb_ts_double_point
	var qdbPointsCount C.qdb_size_t
	err := C.qdb_ts_double_get_ranges(entry.handle, alias, columnName, qdbRanges, qdbRangesCount, &qdbPoints, &qdbPointsCount)

	if err == 0 {
		defer entry.Release(unsafe.Pointer(qdbPoints))
		length := int(qdbPointsCount)
		output := make([]TsDoublePoint, length)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_double_point)(unsafe.Pointer(qdbPoints))[:length:length]
			for i, s := range tmpslice {
				output[i] = s.toStructG()
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}

// GetBlobRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetBlobRanges(column string, ranges ...TsRange) ([]TsBlobPoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	qdbRanges := rangeArrayToC(ranges...)
	var qdbPoints *C.qdb_ts_blob_point
	var qdbPointsCount C.qdb_size_t
	err := C.qdb_ts_blob_get_ranges(entry.handle, alias, columnName, qdbRanges, qdbRangesCount, &qdbPoints, &qdbPointsCount)

	if err == 0 {
		defer entry.Release(unsafe.Pointer(qdbPoints))
		length := int(qdbPointsCount)
		output := make([]TsBlobPoint, length)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_blob_point)(unsafe.Pointer(qdbPoints))[:length:length]
			for i, s := range tmpslice {
				output[i] = s.toStructG()
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}

// DoubleAggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) DoubleAggregate(column string, aggs ...*TsDoubleAggregation) ([]TsDoubleAggregation, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(aggs))
	qdbAggregations := doubleAggregationArrayToC(aggs...)
	aggregations := append([]*TsDoubleAggregation{}, aggs...)
	aggsCopy := make([]TsDoubleAggregation, len(aggs))
	err := C.qdb_ts_double_aggregate(entry.handle, alias, columnName, qdbAggregations, qdbAggregationsCount)
	if err == 0 {
		length := int(qdbAggregationsCount)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_double_aggregation_t)(unsafe.Pointer(qdbAggregations))[:length:length]
			for i, s := range tmpslice {
				*aggregations[i] = s.toStructG()
				aggsCopy[i] = s.toStructG()
			}
		}
	}
	return aggsCopy, makeErrorOrNil(err)
}

// BlobAggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) BlobAggregate(column string, aggs ...*TsBlobAggregation) ([]TsBlobAggregation, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(aggs))
	qdbAggregations := blobAggregationArrayToC(aggs...)
	aggregations := append([]*TsBlobAggregation{}, aggs...)
	aggsCopy := make([]TsBlobAggregation, len(aggs))
	err := C.qdb_ts_blob_aggregate(entry.handle, alias, columnName, qdbAggregations, qdbAggregationsCount)
	if err == 0 {
		length := int(qdbAggregationsCount)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_blob_aggregation_t)(unsafe.Pointer(qdbAggregations))[:length:length]
			for i, s := range tmpslice {
				*aggregations[i] = s.toStructG()
				aggsCopy[i] = s.toStructG()
			}
		}
	}
	return aggsCopy, makeErrorOrNil(err)
}
