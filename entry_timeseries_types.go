package qdb

/*
	#include <qdb/ts.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// :: Start - Column ::

// TsColumnType : Timeseries column types
type TsColumnType C.qdb_ts_column_type_t

// Values
//	tsColumnDouble : column is a double point
//	tsColumnBlob : column is a blob point
const (
	TsColumnUninitialized TsColumnType = C.qdb_ts_column_uninitialized
	TsColumnDouble        TsColumnType = C.qdb_ts_column_double
	TsColumnBlob          TsColumnType = C.qdb_ts_column_blob
)

type tsColumn struct {
	TsColumnInfo
	parent TimeseriesEntry
}

// TsDoubleColumn : a time series double column
type TsDoubleColumn struct {
	tsColumn
}

// TsBlobColumn : a time series blob column
type TsBlobColumn struct {
	tsColumn
}

// :: internals

func (t C.qdb_ts_column_info_t) toStructG(entry TimeseriesEntry) tsColumn {
	return tsColumn{TsColumnInfo{C.GoString(t.name), TsColumnType(t._type)}, entry}
}

func columnArrayToGo(entry TimeseriesEntry, columns *C.qdb_ts_column_info_t, columnsCount C.qdb_size_t) ([]TsDoubleColumn, []TsBlobColumn) {
	length := int(columnsCount)
	doubleColumns := []TsDoubleColumn{}
	blobColumns := []TsBlobColumn{}
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
		for _, s := range tmpslice {
			if s._type == C.qdb_ts_column_double {
				doubleColumns = append(doubleColumns, TsDoubleColumn{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_blob {
				blobColumns = append(blobColumns, TsBlobColumn{s.toStructG(entry)})
			}
		}
	}
	return doubleColumns, blobColumns
}

// :: End - Column ::

// :: Start - Column Information ::

// TsColumnInfo : column information in timeseries
type TsColumnInfo struct {
	name string
	kind TsColumnType
}

// Name : return column name
func (t TsColumnInfo) Name() string {
	return t.name
}

// Type : return column type
func (t TsColumnInfo) Type() TsColumnType {
	return t.kind
}

// NewTsColumnInfo : create a column info structure
func NewTsColumnInfo(columnName string, columnType TsColumnType) TsColumnInfo {
	return TsColumnInfo{columnName, columnType}
}

// :: internals
func (t TsColumnInfo) toStructC() C.qdb_ts_column_info_t {
	// The [4]byte is some sort of padding necessary for Go : struct(char *, int, 4 byte of padding)
	return C.qdb_ts_column_info_t{C.CString(t.name), C.qdb_ts_column_type_t(t.kind), [4]byte{}}
}

func (t C.qdb_ts_column_info_t) toStructInfoG() TsColumnInfo {
	return TsColumnInfo{C.GoString(t.name), TsColumnType(t._type)}
}

func columnInfoArrayToC(cols ...TsColumnInfo) *C.qdb_ts_column_info_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return &columns[0]
}

func columnInfoArrayToGo(columns *C.qdb_ts_column_info_t, columnsCount C.qdb_size_t) []TsColumnInfo {
	length := int(columnsCount)
	columnsInfo := make([]TsColumnInfo, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
		for i, s := range tmpslice {
			columnsInfo[i] = s.toStructInfoG()
		}
	}
	return columnsInfo
}

// :: End - Column Information ::

// :: Start - Data points ::

// :: :: Start - Double Point ::

// TsDoublePoint : timestamped double data point
type TsDoublePoint struct {
	timestamp time.Time
	content   float64
}

// Timestamp : return data point timestamp
func (t TsDoublePoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsDoublePoint) Content() float64 {
	return t.content
}

// NewTsDoublePoint : Create new timeseries double point
func NewTsDoublePoint(timestamp time.Time, value float64) TsDoublePoint {
	return TsDoublePoint{timestamp, value}
}

// :: internals
func (t TsDoublePoint) toStructC() C.qdb_ts_double_point {
	return C.qdb_ts_double_point{toQdbTimespec(t.timestamp), C.double(t.content)}
}

func (t C.qdb_ts_double_point) toStructG() TsDoublePoint {
	return TsDoublePoint{t.timestamp.toStructG(), float64(t.value)}
}

func doublePointArrayToC(pts ...TsDoublePoint) *C.qdb_ts_double_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_double_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func doublePointArrayToGo(points *C.qdb_ts_double_point, pointsCount C.qdb_size_t) []TsDoublePoint {
	length := int(pointsCount)
	output := make([]TsDoublePoint, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_double_point)(unsafe.Pointer(points))[:length:length]
		for i, s := range tmpslice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Double Point ::

// :: :: Start - Blob Point ::

// TsBlobPoint : timestamped data
type TsBlobPoint struct {
	timestamp time.Time
	content   []byte
}

// Timestamp : return data point timestamp
func (t TsBlobPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsBlobPoint) Content() []byte {
	return t.content
}

// NewTsBlobPoint : Create new timeseries double point
func NewTsBlobPoint(timestamp time.Time, value []byte) TsBlobPoint {
	return TsBlobPoint{timestamp, value}
}

// :: internals

// TODO(vianney) : do a better conversion without losing the capacity to pass a pointer
// solution may be in go 1.7: func C.CBytes([]byte) unsafe.Pointer
func (t TsBlobPoint) toStructC() C.qdb_ts_blob_point {
	dataSize := C.qdb_size_t(len(t.content))
	data := unsafe.Pointer(C.CString(string(t.content)))
	return C.qdb_ts_blob_point{toQdbTimespec(t.timestamp), data, dataSize}
}

func (t C.qdb_ts_blob_point) toStructG() TsBlobPoint {
	return TsBlobPoint{t.timestamp.toStructG(), C.GoBytes(t.content, C.int(t.content_length))}
}

func blobPointArrayToC(pts ...TsBlobPoint) *C.qdb_ts_blob_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_blob_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func blobPointArrayToGo(points *C.qdb_ts_blob_point, pointsCount C.qdb_size_t) []TsBlobPoint {
	length := int(pointsCount)
	output := make([]TsBlobPoint, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_blob_point)(unsafe.Pointer(points))[:length:length]
		for i, s := range tmpslice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Double Point ::

// :: End - Data points ::

// :: Start - Range ::

// TsRange : timeseries range with begin and end timestamp
type TsRange struct {
	begin time.Time
	end   time.Time
}

// Begin : returns the start of the time range
func (t TsRange) Begin() time.Time {
	return t.begin
}

// End : returns the end of the time range
func (t TsRange) End() time.Time {
	return t.end
}

// NewRange : creats a time range
func NewRange(begin, end time.Time) TsRange {
	return TsRange{begin, end}
}

// :: internals
func (t TsRange) toStructC() C.qdb_ts_range_t {
	return C.qdb_ts_range_t{begin: toQdbTimespec(t.begin), end: toQdbTimespec(t.end)}
}

func (t C.qdb_ts_range_t) toStructG() TsRange {
	return NewRange(t.begin.toStructG(), t.end.toStructG())
}

func rangeArrayToC(rs ...TsRange) *C.qdb_ts_range_t {
	if len(rs) == 0 {
		return nil
	}
	var ranges []C.qdb_ts_range_t
	for _, r := range rs {
		ranges = append(ranges, r.toStructC())
	}
	return &ranges[0]
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
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsDoublePoint
}

// Type : returns the type of the aggregation
func (t TsDoubleAggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsDoubleAggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsDoubleAggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsDoubleAggregation) Result() TsDoublePoint {
	return t.point
}

// NewDoubleAggregation : Create new timeseries double aggregation
func NewDoubleAggregation(kind TsAggregationType, rng TsRange) *TsDoubleAggregation {
	return &TsDoubleAggregation{kind, rng, 0, TsDoublePoint{}}
}

// :: internals
func (t TsDoubleAggregation) toStructC() C.qdb_ts_double_aggregation_t {
	var cAgg C.qdb_ts_double_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_double_aggregation_t) toStructG() TsDoubleAggregation {
	var gAgg TsDoubleAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t._range.toStructG()
	gAgg.count = int64(t.count)
	gAgg.point = t.result.toStructG()
	return gAgg
}

func doubleAggregationArrayToC(ags ...*TsDoubleAggregation) *C.qdb_ts_double_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var doubleAggregations []C.qdb_ts_double_aggregation_t
	for _, ag := range ags {
		doubleAggregations = append(doubleAggregations, ag.toStructC())
	}
	return &doubleAggregations[0]
}

func doubleAggregationArrayToGo(aggregations *C.qdb_ts_double_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsDoubleAggregation) []TsDoubleAggregation {
	length := int(aggregationsCount)
	output := make([]TsDoubleAggregation, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_double_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
		for i, s := range tmpslice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Double Aggregation

// :: :: Start - Blob Aggregation

// TsBlobAggregation : Aggregation of double type
type TsBlobAggregation struct {
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsBlobPoint
}

// Type : returns the type of the aggregation
func (t TsBlobAggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsBlobAggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsBlobAggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsBlobAggregation) Result() TsBlobPoint {
	return t.point
}

// NewBlobAggregation : Create new timeseries blob aggregation
func NewBlobAggregation(kind TsAggregationType, rng TsRange) *TsBlobAggregation {
	return &TsBlobAggregation{kind, rng, 0, TsBlobPoint{}}
}

// :: internals
func (t TsBlobAggregation) toStructC() C.qdb_ts_blob_aggregation_t {
	var cAgg C.qdb_ts_blob_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_blob_aggregation_t) toStructG() TsBlobAggregation {
	var gAgg TsBlobAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t._range.toStructG()
	gAgg.count = int64(t.count)
	gAgg.point = t.result.toStructG()
	return gAgg
}

func blobAggregationArrayToC(ags ...*TsBlobAggregation) *C.qdb_ts_blob_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var blobAggregations []C.qdb_ts_blob_aggregation_t
	for _, ag := range ags {
		blobAggregations = append(blobAggregations, ag.toStructC())
	}
	return &blobAggregations[0]
}

func blobAggregationArrayToGo(aggregations *C.qdb_ts_blob_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsBlobAggregation) []TsBlobAggregation {
	length := int(aggregationsCount)
	output := make([]TsBlobAggregation, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_blob_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
		for i, s := range tmpslice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Blob Aggregation

// :: End - Aggregation ::
