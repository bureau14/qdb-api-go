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
	columnsArray := columnInfoArrayToC(entry.columns...)
	var columns *C.qdb_ts_column_info_t
	if columnsCount != 0 {
		columns = &columnsArray[0]
	} else {
		columns = nil
	}
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
	var contentPtr *C.qdb_ts_double_point
	var dataPoints []C.qdb_ts_double_point
	if contentCount != 0 {
		dataPoints = doublePointArrayToC(points...)
		contentPtr = &dataPoints[0]
	} else {
		contentPtr = nil
	}
	err := C.qdb_ts_double_insert(entry.handle, alias, columnName, contentPtr, contentCount)
	return makeErrorOrNil(err)
}

// InsertBlob : Inserts blob points in a time series.
//	Time series are distributed across the cluster and support efficient insertion anywhere within the time series as well as efficient lookup based on time.
//	If the time series does not exist, it will be created.
func (entry TimeseriesEntry) InsertBlob(column string, points []TsBlobPoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	contentCount := C.qdb_size_t(len(points))
	var contentPtr *C.qdb_ts_blob_point
	if contentCount != 0 {
		content := make([]C.qdb_ts_blob_point, contentCount)
		for i := C.qdb_size_t(0); i < contentCount; i++ {
			content[i] = points[i].toStructC()
		}
		contentPtr = &content[0]
	} else {
		contentPtr = nil
	}
	err := C.qdb_ts_blob_insert(entry.handle, alias, columnName, contentPtr, contentCount)
	return makeErrorOrNil(err)
}

// TsRanges : multiple timeseries range with begin and end timestamp
type TsRanges []TsRange

func (r TsRanges) toStructC() []C.qdb_ts_range_t {
	var cRanges []C.qdb_ts_range_t
	for index := range r {
		cRanges = append(cRanges, r[index].toStructC())
	}
	return cRanges
}

// GetDoubleRanges : Retrieves blobs in the specitypefied range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetDoubleRanges(column string, ranges ...TsRange) ([]TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	qdbRangesC := rangeArrayToC(ranges...)
	var qdbRanges *C.qdb_ts_range_t
	if len(qdbRangesC) == 0 {
		qdbRanges = nil
	} else {
		qdbRanges = &qdbRangesC[0]
	}
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
	qdbRangesC := rangeArrayToC(ranges...)
	var qdbRanges *C.qdb_ts_range_t
	if len(qdbRangesC) == 0 {
		qdbRanges = nil
	} else {
		qdbRanges = &qdbRangesC[0]
	}
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

// DoubleAggregate : Aggregate a sub-part of a timeseries.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) DoubleAggregate(column string, t TsAggregationType, r TsRange) (TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	var qdbAggregation C.qdb_ts_double_aggregation_t
	qdbAggregation._type = C.qdb_ts_aggregation_type_t(t)
	qdbAggregation._range = r.toStructC()
	err := C.qdb_ts_double_aggregate(entry.handle, alias, columnName, &qdbAggregation, 1)
	timestamp := qdbAggregation.result.timestamp
	result := TsDoublePoint{time.Unix(int64(timestamp.tv_sec), int64(timestamp.tv_nsec)), float64(qdbAggregation.result.value)}
	return result, makeErrorOrNil(err)
}

// DoubleAggregateBatch : Aggregate a sub-part of a timeseries.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) DoubleAggregateBatch(column string, aggs *[]TsDoubleAggregation) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(*aggs))
	qdbAggsC := doubleAggregationArrayToC((*aggs)...)
	var qdbAggregations *C.qdb_ts_double_aggregation_t
	if qdbAggregationsCount != 0 {
		qdbAggregations = &qdbAggsC[0]
	} else {
		qdbAggregations = nil
	}
	err := C.qdb_ts_double_aggregate(entry.handle, alias, columnName, qdbAggregations, qdbAggregationsCount)
	if err == 0 {
		length := int(qdbAggregationsCount)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_double_aggregation_t)(unsafe.Pointer(qdbAggregations))[:length:length]
			for i, s := range tmpslice {
				(*aggs)[i] = s.toStructG()
			}
		}
	}
	return makeErrorOrNil(err)
}

// BlobAggregate : Aggregate a sub-part of a timeseries.
func (entry TimeseriesEntry) BlobAggregate(column string, t TsAggregationType, r TsRange) (TsBlobPoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	var qdbAggregation C.qdb_ts_blob_aggregation_t
	qdbAggregation._type = C.qdb_ts_aggregation_type_t(t)
	qdbAggregation._range = r.toStructC()
	err := C.qdb_ts_blob_aggregate(entry.handle, alias, columnName, &qdbAggregation, 1)
	blob := qdbAggregation.result
	timestamp := blob.timestamp
	result := TsBlobPoint{time.Unix(int64(timestamp.tv_sec), int64(timestamp.tv_nsec)), C.GoBytes(blob.content, C.int(blob.content_length))}
	return result, makeErrorOrNil(err)
}

// BlobAggregateBatch : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) BlobAggregateBatch(column string, aggs *[]TsBlobAggregation) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(*aggs))
	qdbAggsC := blobAggregationArrayToC((*aggs)...)
	var qdbAggregations *C.qdb_ts_blob_aggregation_t
	if qdbAggregationsCount != 0 {
		qdbAggregations = &qdbAggsC[0]
	} else {
		qdbAggregations = nil
	}
	err := C.qdb_ts_blob_aggregate(entry.handle, alias, columnName, qdbAggregations, qdbAggregationsCount)
	if err == 0 {
		length := int(qdbAggregationsCount)
		if length > 0 {
			tmpslice := (*[1 << 30]C.qdb_ts_blob_aggregation_t)(unsafe.Pointer(qdbAggregations))[:length:length]
			for i, s := range tmpslice {
				(*aggs)[i] = s.toStructG()
			}
		}
	}
	return makeErrorOrNil(err)
}
