package qdb

/*
	#include <qdb/ts.h>

	typedef struct
	{
		double min;
		double max;
	} double_range;
*/
import "C"
import (
	"unsafe"
)

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
	cAgg.filtered_range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_double_aggregation_t) toStructG() TsDoubleAggregation {
	var gAgg TsDoubleAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t.filtered_range.toStructG()
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
	cAgg.filtered_range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_blob_aggregation_t) toStructG() TsBlobAggregation {
	var gAgg TsBlobAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t.filtered_range.toStructG()
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

// :: :: Start - Int64 Aggregation

// TsInt64Aggregation : Aggregation of int64 type
type TsInt64Aggregation struct {
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsInt64Point
}

// Type : returns the type of the aggregation
func (t TsInt64Aggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsInt64Aggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsInt64Aggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsInt64Aggregation) Result() TsInt64Point {
	return t.point
}

// NewInt64Aggregation : Create new timeseries int64 aggregation
func NewInt64Aggregation(kind TsAggregationType, rng TsRange) *TsInt64Aggregation {
	return &TsInt64Aggregation{kind, rng, 0, TsInt64Point{}}
}

// :: internals
func (t TsInt64Aggregation) toStructC() C.qdb_ts_int64_aggregation_t {
	var cAgg C.qdb_ts_int64_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg.filtered_range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_int64_aggregation_t) toStructG() TsInt64Aggregation {
	var gAgg TsInt64Aggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t.filtered_range.toStructG()
	gAgg.count = int64(t.count)
	gAgg.point = t.result.toStructG()
	return gAgg
}

func int64AggregationArrayToC(ags ...*TsInt64Aggregation) *C.qdb_ts_int64_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var int64Aggregations []C.qdb_ts_int64_aggregation_t
	for _, ag := range ags {
		int64Aggregations = append(int64Aggregations, ag.toStructC())
	}
	return &int64Aggregations[0]
}

func int64AggregationArrayToGo(aggregations *C.qdb_ts_int64_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsInt64Aggregation) []TsInt64Aggregation {
	length := int(aggregationsCount)
	output := make([]TsInt64Aggregation, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_int64_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
		for i, s := range tmpslice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Int64 Aggregation

// :: :: Start - Timestamp Aggregation

// TsTimestampAggregation : Aggregation of timestamp type
type TsTimestampAggregation struct {
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsTimestampPoint
}

// Type : returns the type of the aggregation
func (t TsTimestampAggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsTimestampAggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsTimestampAggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsTimestampAggregation) Result() TsTimestampPoint {
	return t.point
}

// NewTimestampAggregation : Create new timeseries timestamp aggregation
func NewTimestampAggregation(kind TsAggregationType, rng TsRange) *TsTimestampAggregation {
	return &TsTimestampAggregation{kind, rng, 0, TsTimestampPoint{}}
}

// :: internals
func (t TsTimestampAggregation) toStructC() C.qdb_ts_timestamp_aggregation_t {
	var cAgg C.qdb_ts_timestamp_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg.filtered_range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_timestamp_aggregation_t) toStructG() TsTimestampAggregation {
	var gAgg TsTimestampAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t.filtered_range.toStructG()
	gAgg.count = int64(t.count)
	gAgg.point = t.result.toStructG()
	return gAgg
}

func timestampAggregationArrayToC(ags ...*TsTimestampAggregation) *C.qdb_ts_timestamp_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var timestampAggregations []C.qdb_ts_timestamp_aggregation_t
	for _, ag := range ags {
		timestampAggregations = append(timestampAggregations, ag.toStructC())
	}
	return &timestampAggregations[0]
}

func timestampAggregationArrayToGo(aggregations *C.qdb_ts_timestamp_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsTimestampAggregation) []TsTimestampAggregation {
	length := int(aggregationsCount)
	output := make([]TsTimestampAggregation, length)
	if length > 0 {
		tmpslice := (*[1 << 30]C.qdb_ts_timestamp_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
		for i, s := range tmpslice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// :: :: End - Timestamp Aggregation
