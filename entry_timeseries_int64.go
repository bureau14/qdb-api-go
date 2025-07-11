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

// TsInt64Point : timestamped int64 data point
type TsInt64Point struct {
	timestamp time.Time
	content   int64
}

// Timestamp : return data point timestamp
func (t TsInt64Point) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsInt64Point) Content() int64 {
	return t.content
}

// NewTsInt64Point : Create new timeseries int64 point
func NewTsInt64Point(timestamp time.Time, value int64) TsInt64Point {
	return TsInt64Point{timestamp, value}
}

// :: internals
func (t TsInt64Point) toStructC() C.qdb_ts_int64_point {
	return C.qdb_ts_int64_point{toQdbTimespec(t.timestamp), C.qdb_int_t(t.content)}
}

func TsInt64PointToStructG(t C.qdb_ts_int64_point) TsInt64Point {
	return TsInt64Point{TimespecToStructG(t.timestamp), int64(t.value)}
}

func int64PointArrayToC(pts ...TsInt64Point) *C.qdb_ts_int64_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_int64_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func int64PointArrayToSlice(points *C.qdb_ts_int64_point, length int) []C.qdb_ts_int64_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_int64_point{})]C.qdb_ts_int64_point)(unsafe.Pointer(points))[:length:length]
}

func int64PointArrayToGo(points *C.qdb_ts_int64_point, pointsCount C.qdb_size_t) []TsInt64Point {
	length := int(pointsCount)
	output := make([]TsInt64Point, length)
	if length > 0 {
		slice := int64PointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsInt64PointToStructG(s)
		}
	}
	return output
}

// TsInt64Column : a time series int64 column
type TsInt64Column struct {
	tsColumn
}

// Int64Column : create a column object
func (entry TimeseriesEntry) Int64Column(columnName string) TsInt64Column {
	return TsInt64Column{tsColumn{NewTsColumnInfo(columnName, TsColumnInt64), entry}}
}

// Insert int64 points into a timeseries
func (column TsInt64Column) Insert(points ...TsInt64Point) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := int64PointArrayToC(points...)
	err := C.qdb_ts_int64_insert(column.parent.handle, alias, columnName, content, contentCount)
	return wrapError(err, "timeseries_int64_insert", "alias", column.parent.alias, "column", column.name, "points", len(points))
}

// EraseRanges : erase all points in the specified ranges
func (column TsInt64Column) EraseRanges(rgs ...TsRange) (uint64, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	erasedCount := C.qdb_uint_t(0)
	err := C.qdb_ts_erase_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &erasedCount)
	return uint64(erasedCount), wrapError(err, "ts_int64_erase_ranges", "alias", column.parent.alias, "column", column.name, "ranges", len(rgs))
}

// GetRanges : Retrieves int64s in the specified range of the time series column.
//
//	It is an error to call this function on a non existing time-series.
func (column TsInt64Column) GetRanges(rgs ...TsRange) ([]TsInt64Point, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_int64_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_int64_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return int64PointArrayToGo(points, pointsCount), nil
	}
	return nil, wrapError(err, "ts_int64_get_ranges", "alias", column.parent.alias, "column", column.name, "ranges", len(rgs))
}

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
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func TsInt64AggregationToStructG(t C.qdb_ts_int64_aggregation_t) TsInt64Aggregation {
	var gAgg TsInt64Aggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = TsRangeToStructG(t._range)
	gAgg.count = int64(t.count)
	gAgg.point = TsInt64PointToStructG(t.result)
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

func int64AggregationArrayToSlice(aggregations *C.qdb_ts_int64_aggregation_t, length int) []C.qdb_ts_int64_aggregation_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_int64_aggregation_t{})]C.qdb_ts_int64_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
}

func int64AggregationArrayToGo(aggregations *C.qdb_ts_int64_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsInt64Aggregation) []TsInt64Aggregation {
	length := int(aggregationsCount)
	output := make([]TsInt64Aggregation, length)
	if length > 0 {
		slice := int64AggregationArrayToSlice(aggregations, length)
		for i, s := range slice {
			*aggs[i] = TsInt64AggregationToStructG(s)
			output[i] = TsInt64AggregationToStructG(s)
		}
	}
	return output
}

// TODO(Vianney): Implement aggregate

// Aggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
//
//	It is an error to call this function on a non existing time-series.
func (column TsInt64Column) Aggregate(aggs ...*TsInt64Aggregation) ([]TsInt64Aggregation, error) {
	return nil, ErrNotImplemented
}

// GetInt64 : gets an int64 in row
func (t *TsBulk) GetInt64() (int64, error) {
	var content C.qdb_int_t
	err := C.qdb_ts_row_get_int64(t.table, C.qdb_size_t(t.index), &content)
	t.index++
	return int64(content), wrapError(err, "ts_bulk_get_int64")
}

// RowSetInt64 : Set int64 at specified index in current row
func (t *TsBatch) RowSetInt64(index, value int64) error {
	valueIndex := C.qdb_size_t(index)
	return wrapError(C.qdb_ts_batch_row_set_int64(t.table, valueIndex, C.qdb_int_t(value)), "ts_batch_row_set_int64", "index", valueIndex, "value", value)
}
