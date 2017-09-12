package qdb

/*
	#include <qdb/ts.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// SizeType typedef qdb_size_t
type SizeType C.qdb_size_t

// :: Start - Column Information ::

// TsColumnType : Timeseries column types
type TsColumnType C.qdb_ts_column_type_t

// Values
//	TsColumnDouble : column is a double point
//	TsColumnBlob : column is a blob point
const (
	TsColumnDouble TsColumnType = C.qdb_ts_column_double
	TsColumnBlob   TsColumnType = C.qdb_ts_column_blob
)

// TsColumnInfo : column information in timeseries
type TsColumnInfo struct {
	n string
	t TsColumnType
}

// Name : return column name
func (t TsColumnInfo) Name() string {
	return t.n
}

// Type : return column type
func (t TsColumnInfo) Type() TsColumnType {
	return t.t
}

// NewTsColumnInfo : create a column info structure
func NewTsColumnInfo(columnName string, columnType TsColumnType) TsColumnInfo {
	return TsColumnInfo{columnName, columnType}
}

// :: internals
func (t TsColumnInfo) toStructC() C.qdb_ts_column_info_t {
	// The [4]byte is some sort of padding necessary for Go : struct(char *, int, 4 byte of padding)
	return C.qdb_ts_column_info_t{C.CString(t.n), C.qdb_ts_column_type_t(t.t), [4]byte{}}
}

func (t C.qdb_ts_column_info_t) toStructG() TsColumnInfo {
	return TsColumnInfo{C.GoString(t.name), TsColumnType(t._type)}
}

func columnInfoArrayToC(cols ...TsColumnInfo) []C.qdb_ts_column_info_t {
	columns := make([]C.qdb_ts_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return columns
}

// :: End - Column Information ::

// :: Start - Data points ::

// :: :: Start - Double Point ::

// TsDoublePoint : timestamped double data point
type TsDoublePoint struct {
	t time.Time
	c float64
}

// Timestamp : return data point timestamp
func (t TsDoublePoint) Timestamp() time.Time {
	return t.t
}

// Content : return data point content
func (t TsDoublePoint) Content() float64 {
	return t.c
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp time.Time, value float64) TsDoublePoint {
	return TsDoublePoint{timestamp, value}
}

// :: internals
func (t TsDoublePoint) toStructC() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{toQdbTimespec(t.t), C.double(t.c)}
}

func (t C.qdb_ts_double_point) toStructG() TsDoublePoint {
	return TsDoublePoint{t.timestamp.toStructG(), float64(t.value)}
}

func doublePointArrayToC(pts ...TsDoublePoint) []C.qdb_ts_double_point {
	points := make([]C.qdb_ts_double_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return points
}

// :: :: End - Double Point ::

// :: :: Start - Blob Point ::

// TsBlobPoint : timestamped data
type TsBlobPoint struct {
	t time.Time
	c []byte
}

// Timestamp : return data point timestamp
func (t TsBlobPoint) Timestamp() time.Time {
	return t.t
}

// Content : return data point content
func (t TsBlobPoint) Content() []byte {
	return t.c
}

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp time.Time, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
}

// :: internals

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (t TsBlobPoint) toStructC() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(t.c))
	data := unsafe.Pointer(C.CString(string(t.c)))
	return C.qdb_ts_blob_point{toQdbTimespec(t.t), data, dataSize}
}

func (t C.qdb_ts_blob_point) toStructG() TsBlobPoint {
	return TsBlobPoint{t.timestamp.toStructG(), C.GoBytes(t.content, C.int(t.content_length))}
}

// :: :: End - Double Point ::

// :: End - Data points ::

// :: Start - Range ::

// TsRange : timeseries range with begin and end timestamp
type TsRange struct {
	b time.Time
	e time.Time
}

// Begin : returns the start of the time range
func (t TsRange) Begin() time.Time {
	return t.b
}

// End : returns the end of the time range
func (t TsRange) End() time.Time {
	return t.e
}

// NewRange : creats a time range
func NewRange(begin, end time.Time) TsRange {
	return TsRange{begin, end}
}

// :: internals
func (t TsRange) toStructC() C.qdb_ts_range_t {
	return C.qdb_ts_range_t{toQdbTimespec(t.b), toQdbTimespec(t.e)}
}

func (t C.qdb_ts_range_t) toStructG() TsRange {
	return NewRange(t.begin.toStructG(), t.end.toStructG())
}

func rangeArrayToC(rs ...TsRange) []C.qdb_ts_range_t {
	var ranges []C.qdb_ts_range_t
	for _, r := range rs {
		ranges = append(ranges, r.toStructC())
	}
	return ranges
}

// :: End - Range ::

// :: Start - Aggregation ::

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

// :: :: Start - Double Aggregation

// TsDoubleAggregation : Aggregation of double type
type TsDoubleAggregation struct {
	t TsAggregationType
	r TsRange
	s SizeType
	p TsDoublePoint
}

// Type : returns the type of the aggregation
func (t TsDoubleAggregation) Type() TsAggregationType {
	return t.t
}

// Range : returns the range of the aggregation
func (t TsDoubleAggregation) Range() TsRange {
	return t.r
}

// Count : returns the number of points aggregated into the result
func (t TsDoubleAggregation) Count() SizeType {
	return t.s
}

// Result : result of the aggregation
func (t TsDoubleAggregation) Result() TsDoublePoint {
	return t.p
}

// NewDoubleAggregation : Create new timeseries double aggregation
func NewDoubleAggregation(t TsAggregationType, r TsRange) TsDoubleAggregation {
	return TsDoubleAggregation{t, r, 0, TsDoublePoint{}}
}

// :: internals
func (t TsDoubleAggregation) toStructC() C.qdb_ts_double_aggregation_t {
	var cAgg C.qdb_ts_double_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.t)
	cAgg._range = t.r.toStructC()
	cAgg.count = C.qdb_size_t(t.s)
	cAgg.result = t.p.toStructC()
	return cAgg
}

func (t C.qdb_ts_double_aggregation_t) toStructG() TsDoubleAggregation {
	var gAgg TsDoubleAggregation
	gAgg.t = TsAggregationType(t._type)
	gAgg.r = t._range.toStructG()
	gAgg.s = SizeType(t.count)
	gAgg.p = t.result.toStructG()
	return gAgg
}

func doubleAggregationArrayToC(ags ...TsDoubleAggregation) []C.qdb_ts_double_aggregation_t {
	var doubleAggregations []C.qdb_ts_double_aggregation_t
	for _, ag := range ags {
		doubleAggregations = append(doubleAggregations, ag.toStructC())
	}
	return doubleAggregations
}

// :: :: End - Double Aggregation

// :: :: Start - Blob Aggregation

// TsBlobAggregation : Aggregation of double type
type TsBlobAggregation struct {
	t TsAggregationType
	r TsRange
	s SizeType
	p TsBlobPoint
}

// Type : returns the type of the aggregation
func (t TsBlobAggregation) Type() TsAggregationType {
	return t.t
}

// Range : returns the range of the aggregation
func (t TsBlobAggregation) Range() TsRange {
	return t.r
}

// Count : returns the number of points aggregated into the result
func (t TsBlobAggregation) Count() SizeType {
	return t.s
}

// Result : result of the aggregation
func (t TsBlobAggregation) Result() TsBlobPoint {
	return t.p
}

// NewBlobAggregation : Create new timeseries blob aggregation
func NewBlobAggregation(t TsAggregationType, r TsRange) TsBlobAggregation {
	return TsBlobAggregation{t, r, 0, TsBlobPoint{}}
}

// :: internals
func (t TsBlobAggregation) toStructC() C.qdb_ts_blob_aggregation_t {
	var cAgg C.qdb_ts_blob_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.t)
	cAgg._range = t.r.toStructC()
	cAgg.count = C.qdb_size_t(t.s)
	cAgg.result = t.p.toStructC()
	return cAgg
}

func (t C.qdb_ts_blob_aggregation_t) toStructG() TsBlobAggregation {
	var gAgg TsBlobAggregation
	gAgg.t = TsAggregationType(t._type)
	gAgg.r = t._range.toStructG()
	gAgg.s = SizeType(t.count)
	gAgg.p = t.result.toStructG()
	return gAgg
}

func blobAggregationArrayToC(ags ...TsBlobAggregation) []C.qdb_ts_blob_aggregation_t {
	var blobAggregations []C.qdb_ts_blob_aggregation_t
	for _, ag := range ags {
		blobAggregations = append(blobAggregations, ag.toStructC())
	}
	return blobAggregations
}

// :: :: End - Blob Aggregation

// :: End - Aggregation ::
