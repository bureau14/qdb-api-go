package qdb

/*
	#include <qdb/ts.h>
	#include <qdb/error.h>
	#include <qdb/client.h>
*/
import "C"
import "unsafe"

// TsColumnInfo : column information in timeseries
type TsColumnInfo C.qdb_ts_column_info_t

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
	columnsCount := C.qdb_size_t(len(entry.columns))
	var columns unsafe.Pointer
	if columnsCount != 0 {
		columns = unsafe.Pointer((*C.qdb_ts_column_info_t)(&entry.columns[0]))
	} else {

		columns = unsafe.Pointer(nil)
	}
	err := C.qdb_ts_create(entry.handle, alias, (*C.struct_qdb_ts_column_info)(columns), columnsCount)
	return makeErrorOrNil(err)
}

// TsDoublePoint : timestamped data
type TsDoublePoint struct {
	Timestamp TimespecType
	Content   float64
}

func (ts TsDoublePoint) toQdbDoublePoint() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{ts.Timestamp.toQdbTimespec(), C.double(ts.Content)}
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp TimespecType, value float64) TsDoublePoint {
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
			content[i] = points[i].toQdbDoublePoint()
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
	Timestamp TimespecType
	Content   []byte
}

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (ts TsBlobPoint) toQdbBlobPoint() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(ts.Content))
	data := unsafe.Pointer(C.CString(string(ts.Content)))
	return C.qdb_ts_blob_point{ts.Timestamp.toQdbTimespec(), data, dataSize}
}

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp TimespecType, value []byte) TsBlobPoint {
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
			content[i] = points[i].toQdbBlobPoint()
		}
		contentPtr = &content[0]
	} else {
		contentPtr = nil
	}
	err := C.qdb_ts_blob_insert(entry.handle, alias, columnName, contentPtr, contentCount)
	return makeErrorOrNil(err)
}

// TsRange : timeseries range with begin and end timestamp
type TsRange C.qdb_ts_range_t

// NewTsRange : Create new timeseries range
func NewTsRange(begin, end TimespecType) TsRange {
	return TsRange{begin.toQdbTimespec(), end.toQdbTimespec()}
}

// GetDoubleRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetDoubleRanges(column string, ranges []TsRange) ([]TsDoublePoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	var qdbRanges *C.qdb_ts_range_t
	if qdbRangesCount != 0 {
		qdbRanges = (*C.qdb_ts_range_t)(unsafe.Pointer(&ranges[0]))
	} else {
		qdbRanges = nil
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
				output[i] = TsDoublePoint{s.timestamp.toTimeSpec(), float64(s.value)}
			}
		}
		return output, nil
	}
	return nil, ErrorType(err)
}

// GetBlobRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetBlobRanges(column string, ranges []TsRange) ([]TsBlobPoint, error) {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbRangesCount := C.qdb_size_t(len(ranges))
	var qdbRanges *C.qdb_ts_range_t
	if qdbRangesCount != 0 {
		qdbRanges = (*C.qdb_ts_range_t)(unsafe.Pointer(&ranges[0]))
	} else {
		qdbRanges = nil
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
				output[i] = TsBlobPoint{s.timestamp.toTimeSpec(), C.GoBytes(s.content, C.int(s.content_length))}
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

// GetDoubleAggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetDoubleAggregate(column string, aggs *[]TsDoubleAggregation) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(*aggs))
	var qdbAggregations *C.qdb_ts_double_aggregation_t
	if qdbAggregationsCount != 0 {
		qdbAggregations = (*C.qdb_ts_double_aggregation_t)(unsafe.Pointer(&((*aggs)[0])))
	} else {
		qdbAggregations = nil
	}
	err := C.qdb_ts_double_aggregate(entry.handle, alias, columnName, qdbAggregations, qdbAggregationsCount)
	return makeErrorOrNil(err)
}

// TsBlobAggregation : Aggregation of double type
type TsBlobAggregation struct {
	T TsAggregationType
	R TsRange
	S SizeType
	P TsBlobPoint
}

// GetBlobAggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (entry TimeseriesEntry) GetBlobAggregate(column string, aggs *[]TsBlobAggregation) error {
	alias := C.CString(entry.alias)
	columnName := C.CString(column)
	qdbAggregationsCount := C.qdb_size_t(len(*aggs))
	var qdbAggregations *C.qdb_ts_blob_aggregation_t
	if qdbAggregationsCount != 0 {
		qdbAggregations = (*C.qdb_ts_blob_aggregation_t)(unsafe.Pointer(&((*aggs)[0])))
	} else {
		qdbAggregations = nil
	}
	err := C.qdb_ts_blob_aggregate(entry.handle, alias, columnName, qdbAggregations, qdbAggregationsCount)
	return makeErrorOrNil(err)
}
