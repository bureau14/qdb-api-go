package qdb

/*
	#include <qdb/ts.h>
*/
import "C"

import (
	"math"
	"time"
	"unsafe"
)

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

func TsDoublePointToStructG(t C.qdb_ts_double_point) TsDoublePoint {
	return TsDoublePoint{TimespecToStructG(t.timestamp), float64(t.value)}
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

func doublePointArrayToSlice(points *C.qdb_ts_double_point, length int) []C.qdb_ts_double_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_double_point{})]C.qdb_ts_double_point)(unsafe.Pointer(points))[:length:length]
}

func doublePointArrayToGo(points *C.qdb_ts_double_point, pointsCount C.qdb_size_t) []TsDoublePoint {
	length := int(pointsCount)
	output := make([]TsDoublePoint, length)
	if length > 0 {
		slice := doublePointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsDoublePointToStructG(s)
		}
	}
	return output
}

// TsDoubleColumn : a time series double column
type TsDoubleColumn struct {
	tsColumn
}

// DoubleColumn : create a column object
func (entry TimeseriesEntry) DoubleColumn(columnName string) TsDoubleColumn {
	return TsDoubleColumn{tsColumn{NewTsColumnInfo(columnName, TsColumnDouble), entry}}
}

// Insert double points into a timeseries
func (column TsDoubleColumn) Insert(points ...TsDoublePoint) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := doublePointArrayToC(points...)
	err := C.qdb_ts_double_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// EraseRanges : erase all points in the specified ranges
func (column TsDoubleColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	erasedCount := C.qdb_uint_t(0)
	err := C.qdb_ts_erase_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &erasedCount)
	return uint64(erasedCount), makeErrorOrNil(err)
}

// GetRanges : Retrieves blobs in the specified range of the time series column.
//
//	It is an error to call this function on a non existing time-series.
func (column TsDoubleColumn) GetRanges(rgs ...TsRange) ([]TsDoublePoint, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
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

func TsDoubleAggregationToStructG(t C.qdb_ts_double_aggregation_t) TsDoubleAggregation {
	var gAgg TsDoubleAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = TsRangeToStructG(t._range)
	gAgg.count = int64(t.count)
	gAgg.point = TsDoublePointToStructG(t.result)
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

func doubleAggregationArrayToSlice(aggregations *C.qdb_ts_double_aggregation_t, length int) []C.qdb_ts_double_aggregation_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_double_aggregation_t{})]C.qdb_ts_double_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
}

func doubleAggregationArrayToGo(aggregations *C.qdb_ts_double_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsDoubleAggregation) []TsDoubleAggregation {
	length := int(aggregationsCount)
	output := make([]TsDoubleAggregation, length)
	if length > 0 {
		slice := doubleAggregationArrayToSlice(aggregations, length)
		for i, s := range slice {
			*aggs[i] = TsDoubleAggregationToStructG(s)
			output[i] = TsDoubleAggregationToStructG(s)
		}
	}
	return output
}

// Aggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
//
//	It is an error to call this function on a non existing time-series.
func (column TsDoubleColumn) Aggregate(aggs ...*TsDoubleAggregation) ([]TsDoubleAggregation, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	aggregations := doubleAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsDoubleAggregation
	err := C.qdb_ts_double_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = doubleAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}

// GetDouble : gets a double in row
func (t *TsBulk) GetDouble() (float64, error) {
	var content C.double
	err := C.qdb_ts_row_get_double(t.table, C.qdb_size_t(t.index), &content)
	t.index++
	return float64(content), makeErrorOrNil(err)
}

// RowSetDouble : Set double at specified index in current row
func (t *TsBatch) RowSetDouble(index int64, value float64) error {
	valueIndex := C.qdb_size_t(index)
	return makeErrorOrNil(C.qdb_ts_batch_row_set_double(t.table, valueIndex, C.double(value)))
}
