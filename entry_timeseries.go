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

// SizeType typedef qdb_size_t
type SizeType C.qdb_size_t

// TsColumnInfo : column information in timeseries
type TsColumnInfo struct {
	Name string
	Type TsColumnType
}

func (tsCI TsColumnInfo) toStructC() C.qdb_ts_column_info_t {
	// The [4]byte is some sort of padding necessary for Go : struct(char *, int, 4 byte of padding)
	return C.qdb_ts_column_info_t{C.CString(tsCI.Name), C.qdb_ts_column_type_t(tsCI.Type), [4]byte{}}
}

func (cval C.qdb_ts_column_info_t) toStructG() TsColumnInfo {
	return TsColumnInfo{C.GoString(cval.name), TsColumnType(cval._type)}
}

// TsColumnInfos : multiple column information in timeseries
type TsColumnInfos []TsColumnInfo

func (r TsColumnInfos) toStructC() []C.qdb_ts_column_info_t {
	var cColumnsInfos []C.qdb_ts_column_info_t
	for index := range r {
		cColumnsInfos = append(cColumnsInfos, r[index].toStructC())
	}
	return cColumnsInfos
}

// TsColumnType : Timeseries column types
type TsColumnType C.qdb_ts_column_type_t

// Values
//	TsColumnDouble : column is a double point
//	TsColumnBlob : column is a blob point
const (
	TsColumnDouble TsColumnType = C.qdb_ts_column_double
	TsColumnBlob   TsColumnType = C.qdb_ts_column_blob
)

// NewTsColumnInfo : create a column info structure
func NewTsColumnInfo(columnName string, columnType TsColumnType) TsColumnInfo {
	return TsColumnInfo{columnName, columnType}
}

// TimeseriesEntry : timeseries double entry data type
type TimeseriesEntry struct {
	Entry
	columns TsColumnInfos
}

// Create : create a new timeseries
func (entry TimeseriesEntry) Create() error {
	alias := C.CString(entry.alias)
	columnsCount := C.qdb_size_t(len(entry.columns))
	columnsArray := entry.columns.toStructC()
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

// InsertColumns : insert columns in a existing timeseries
func (entry *TimeseriesEntry) InsertColumns(cols ...TsColumnInfo) error {
	colsToInsert := TsColumnInfos(cols)
	alias := C.CString(entry.alias)
	columnsCount := C.qdb_size_t(len(colsToInsert))
	columnsArray := colsToInsert.toStructC()
	var columns *C.qdb_ts_column_info_t
	if columnsCount != 0 {
		columns = &columnsArray[0]
	} else {
		columns = nil
	}
	err := C.qdb_ts_insert_columns(entry.handle, alias, columns, columnsCount)
	if err == 0 {
		length := int(columnsCount)
		if length > 0 {
			previousLength := len(entry.columns)
			entry.columns = append(entry.columns, cols...)
			tmpslice := (*[1 << 30]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
			for i, s := range tmpslice {
				entry.columns[i+previousLength] = s.toStructG()
			}
		}
	}
	return makeErrorOrNil(err)
}

// TsDoublePoint : timestamped data
type TsDoublePoint struct {
	Timestamp time.Time
	Content   float64
}

func (dp TsDoublePoint) toStructC() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{toQdbTimespec(dp.Timestamp), C.double(dp.Content)}
}

func (cval C.qdb_ts_double_point) toStructG() TsDoublePoint {
	return TsDoublePoint{cval.timestamp.toStructG(), float64(cval.value)}
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp time.Time, value float64) TsDoublePoint {
	return TsDoublePoint{timestamp, value}
}

// InsertDouble : Inserts double points in a time series.
//	Time series are distributed across the cluster and support efficient insertion anywhere within the time series as well as efficient lookup based on time.
//	If the time series does not exist, it will be created.
func (entry TimeseriesEntry) InsertDouble(column string, points []TsDoublePoint) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	contentCount := C.qdb_size_t(len(points))
	var contentPtr *C.qdb_ts_double_point
	if contentCount != 0 {
		content := make([]C.qdb_ts_double_point, contentCount)
		for i := C.qdb_size_t(0); i < contentCount; i++ {
			content[i] = points[i].toStructC()
		}
		contentPtr = &content[0]
	} else {
		contentPtr = nil
	}
	err := C.qdb_ts_double_insert(entry.handle, alias, columnName, contentPtr, contentCount)
	return makeErrorOrNil(err)
}

// TsBlobPoint : timestamped data
type TsBlobPoint struct {
	Timestamp time.Time
	Content   []byte
}

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (ts TsBlobPoint) toStructC() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(ts.Content))
	data := unsafe.Pointer(C.CString(string(ts.Content)))
	return C.qdb_ts_blob_point{toQdbTimespec(ts.Timestamp), data, dataSize}
}

func (cval C.qdb_ts_blob_point) toStructG() TsBlobPoint {
	return TsBlobPoint{cval.timestamp.toStructG(), C.GoBytes(cval.content, C.int(cval.content_length))}
}

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp time.Time, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
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

// TsRange : timeseries range with begin and end timestamp
type TsRange struct {
	Begin time.Time
	End   time.Time
}

func (r TsRange) toStructC() C.qdb_ts_range_t {
	return C.qdb_ts_range_t{toQdbTimespec(r.Begin), toQdbTimespec(r.End)}
}

func (cval C.qdb_ts_range_t) toStructG() TsRange {
	return TsRange{cval.begin.toStructG(), cval.end.toStructG()}
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
func (entry TimeseriesEntry) GetDoubleRanges(column string, ranges TsRanges) ([]TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	qdbRangesC := ranges.toStructC()
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
func (entry TimeseriesEntry) GetBlobRanges(column string, ranges TsRanges) ([]TsBlobPoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	qdbRangesC := ranges.toStructC()
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

// TsAggregationType typedef of C.qdb_ts_aggregation_type
type TsAggregationType C.qdb_ts_aggregation_type_t

// Each type gets its value between the begin and end timestamps of aggregation
const (
	AggFirst              TsAggregationType = C.qdb_agg_first
	AggLast               TsAggregationType = C.qdb_agg_last
	AggMin                TsAggregationType = C.qdb_agg_min
	AggMax                TsAggregationType = C.qdb_agg_max
	AggArithmeticMean     TsAggregationType = C.qdb_agg_arithmetic_mean
	AggHarmonicMean       TsAggregationType = C.qdb_agg_harmonic_mean
	AggGeometricMean      TsAggregationType = C.qdb_agg_geometric_mean
	AggQuadraticMean      TsAggregationType = C.qdb_agg_quadratic_mean
	AggCount              TsAggregationType = C.qdb_agg_count
	AggSum                TsAggregationType = C.qdb_agg_sum
	AggSumOfSquares       TsAggregationType = C.qdb_agg_sum_of_squares
	AggSpread             TsAggregationType = C.qdb_agg_spread
	AggSampleVariance     TsAggregationType = C.qdb_agg_sample_variance
	AggSampleStddev       TsAggregationType = C.qdb_agg_sample_stddev
	AggPopulationVariance TsAggregationType = C.qdb_agg_population_variance
	AggPopulationStddev   TsAggregationType = C.qdb_agg_population_stddev
	AggAbsMin             TsAggregationType = C.qdb_agg_abs_min
	AggAbsMax             TsAggregationType = C.qdb_agg_abs_max
	AggProduct            TsAggregationType = C.qdb_agg_product
	AggSkewness           TsAggregationType = C.qdb_agg_skewness
	AggKurtosis           TsAggregationType = C.qdb_agg_kurtosis
)

// TsDoubleAggregation : Aggregation of double type
type TsDoubleAggregation struct {
	T TsAggregationType
	R TsRange
	S SizeType
	P TsDoublePoint
}

func (agg TsDoubleAggregation) toStructC() C.qdb_ts_double_aggregation_t {
	var cAgg C.qdb_ts_double_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(agg.T)
	cAgg._range = agg.R.toStructC()
	cAgg.count = C.qdb_size_t(agg.S)
	cAgg.result = agg.P.toStructC()
	return cAgg
}

func (cval C.qdb_ts_double_aggregation_t) toStructG() TsDoubleAggregation {
	var gAgg TsDoubleAggregation
	gAgg.T = TsAggregationType(cval._type)
	gAgg.R = cval._range.toStructG()
	gAgg.S = SizeType(cval.count)
	gAgg.P = cval.result.toStructG()
	return gAgg
}

// TsDoubleAggregations : Multiple aggregation of double type
type TsDoubleAggregations []TsDoubleAggregation

func (aggs TsDoubleAggregations) toStructC() []C.qdb_ts_double_aggregation_t {
	var cAggs []C.qdb_ts_double_aggregation_t
	for index := range aggs {
		cAggs = append(cAggs, aggs[index].toStructC())
	}
	return cAggs
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

// DoubleAggregates : Aggregate a sub-part of a timeseries.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) DoubleAggregates(column string, aggs *TsDoubleAggregations) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(*aggs))
	qdbAggsC := aggs.toStructC()
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

// TsBlobAggregation : Aggregation of double type
type TsBlobAggregation struct {
	T TsAggregationType
	R TsRange
	S SizeType
	P TsBlobPoint
}

func (agg TsBlobAggregation) toStructC() C.qdb_ts_blob_aggregation_t {
	var cAgg C.qdb_ts_blob_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(agg.T)
	cAgg._range = agg.R.toStructC()
	cAgg.count = C.qdb_size_t(agg.S)
	cAgg.result = agg.P.toStructC()
	return cAgg
}

func (cval C.qdb_ts_blob_aggregation_t) toStructG() TsBlobAggregation {
	var gAgg TsBlobAggregation
	gAgg.T = TsAggregationType(cval._type)
	gAgg.R = cval._range.toStructG()
	gAgg.S = SizeType(cval.count)
	gAgg.P = cval.result.toStructG()
	return gAgg
}

// TsBlobAggregations : Multiple aggregation of double type
type TsBlobAggregations []TsBlobAggregation

func (aggs TsBlobAggregations) toStructC() []C.qdb_ts_blob_aggregation_t {
	var cAggs []C.qdb_ts_blob_aggregation_t
	for index := range aggs {
		cAggs = append(cAggs, aggs[index].toStructC())
	}
	return cAggs
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

// BlobAggregates : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) BlobAggregates(column string, aggs *TsBlobAggregations) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(*aggs))
	qdbAggsC := aggs.toStructC()
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
