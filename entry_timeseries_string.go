package qdb

/*
	#include <qdb/ts.h>
	#include <stdlib.h>
*/
import "C"

import (
	"math"
	"time"
	"unsafe"
)

// TsStringPoint : timestamped data
type TsStringPoint struct {
	timestamp time.Time
	content   string
}

// NewTsStringPoint : Create new timeseries string point
func NewTsStringPoint(timestamp time.Time, value string) TsStringPoint {
	return TsStringPoint{timestamp, value}
}

// Timestamp : return data point timestamp
func (t TsStringPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsStringPoint) Content() string {
	return t.content
}

// :: internals
func (t TsStringPoint) toStructC() C.qdb_ts_string_point {
	dataSize := C.qdb_size_t(len(t.content))
	data := convertToCharStar(string(t.content))

	return C.qdb_ts_string_point{toQdbTimespec(t.timestamp), data, dataSize}
}

func TsStringPointToStructG(t C.qdb_ts_string_point) TsStringPoint {
	return TsStringPoint{TimespecToStructG(t.timestamp), C.GoStringN(t.content, C.int(t.content_length))}
}

func stringPointArrayToC(pts ...TsStringPoint) *C.qdb_ts_string_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_string_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}

	return &points[0]
}

func releaseStringPointArray(points *C.qdb_ts_string_point, length int) {
	if length > 0 {
		slice := stringPointArrayToSlice(points, length)
		for _, s := range slice {
			C.free(unsafe.Pointer(s.content))
		}
	}
}

func stringPointArrayToSlice(points *C.qdb_ts_string_point, length int) []C.qdb_ts_string_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.

	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_string_point{})]C.qdb_ts_string_point)(unsafe.Pointer(points))[:length:length]
}

func stringPointArrayToGo(points *C.qdb_ts_string_point, pointsCount C.qdb_size_t) []TsStringPoint {
	length := int(pointsCount)
	output := make([]TsStringPoint, length)
	if length > 0 {
		slice := stringPointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = TsStringPointToStructG(s)
		}
	}

	return output
}

// TsStringColumn : a time series string column
type TsStringColumn struct {
	tsColumn
}

// StringColumn : create a column object
func (entry TimeseriesEntry) StringColumn(columnName string) TsStringColumn {
	return TsStringColumn{tsColumn{NewTsColumnInfo(columnName, TsColumnString), entry}}
}

// SymbolColumn : create a column object (the symbol table name is not set)
func (entry TimeseriesEntry) SymbolColumn(columnName, symtableName string) TsStringColumn {
	return TsStringColumn{tsColumn{NewSymbolColumnInfo(columnName, symtableName), entry}}
}

// Insert string points into a timeseries
func (column TsStringColumn) Insert(points ...TsStringPoint) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := stringPointArrayToC(points...)
	defer releaseStringPointArray(content, len(points))
	err := C.qdb_ts_string_insert(column.parent.handle, alias, columnName, content, contentCount)

	return wrapError(err, "timeseries_string_insert", "alias", column.parent.alias, "column", column.name, "points", len(points))
}

// EraseRanges : erase all points in the specified ranges
func (column TsStringColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	erasedCount := C.qdb_uint_t(0)
	err := C.qdb_ts_erase_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &erasedCount)

	return uint64(erasedCount), wrapError(err, "ts_string_erase_ranges", "alias", column.parent.alias, "column", column.name, "ranges", len(rgs))
}

// GetRanges : Retrieves strings in the specified range of the time series column.
//
//	It is an error to call this function on a non existing time-series.
func (column TsStringColumn) GetRanges(rgs ...TsRange) ([]TsStringPoint, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_string_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_string_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))

		return stringPointArrayToGo(points, pointsCount), nil
	}

	return nil, wrapError(err, "ts_string_get_ranges", "alias", column.parent.alias, "column", column.name, "ranges", len(rgs))
}

// TsStringAggregation : Aggregation of double type
type TsStringAggregation struct {
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsStringPoint
}

// NewStringAggregation : Create new timeseries string aggregation
func NewStringAggregation(kind TsAggregationType, rng TsRange) *TsStringAggregation {
	return &TsStringAggregation{kind, rng, 0, TsStringPoint{}}
}

// Type : returns the type of the aggregation
func (t TsStringAggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsStringAggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsStringAggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsStringAggregation) Result() TsStringPoint {
	return t.point
}

// :: internals
func (t TsStringAggregation) toStructC() C.qdb_ts_string_aggregation_t {
	var cAgg C.qdb_ts_string_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()

	return cAgg
}

func TsStringAggregationToStructG(t C.qdb_ts_string_aggregation_t) TsStringAggregation {
	var gAgg TsStringAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = TsRangeToStructG(t._range)
	gAgg.count = int64(t.count)
	gAgg.point = TsStringPointToStructG(t.result)

	return gAgg
}

func stringAggregationArrayToC(ags ...*TsStringAggregation) *C.qdb_ts_string_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var stringAggregations []C.qdb_ts_string_aggregation_t
	for _, ag := range ags {
		stringAggregations = append(stringAggregations, ag.toStructC())
	}

	return &stringAggregations[0]
}

func stringAggregationArrayToSlice(aggregations *C.qdb_ts_string_aggregation_t, length int) []C.qdb_ts_string_aggregation_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.

	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_string_aggregation_t{})]C.qdb_ts_string_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
}

func stringAggregationArrayToGo(aggregations *C.qdb_ts_string_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsStringAggregation) []TsStringAggregation {
	length := int(aggregationsCount)
	output := make([]TsStringAggregation, length)
	if length > 0 {
		slice := stringAggregationArrayToSlice(aggregations, length)
		for i, s := range slice {
			*aggs[i] = TsStringAggregationToStructG(s)
			output[i] = TsStringAggregationToStructG(s)
		}
	}

	return output
}

// Aggregate : Aggregate a sub-part of the time series.
//
//	It is an error to call this function on a non existing time-series.
func (column TsStringColumn) Aggregate(aggs ...*TsStringAggregation) ([]TsStringAggregation, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	aggregations := stringAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsStringAggregation
	err := C.qdb_ts_string_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = stringAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}

	return output, makeErrorOrNil(err)
}

// GetString : gets a string in row
func (t *TsBulk) GetString() (string, error) {
	var content *C.char
	defer t.h.Release(unsafe.Pointer(content))
	var contentLength C.qdb_size_t
	err := C.qdb_ts_row_get_string(t.table, C.qdb_size_t(t.index), &content, &contentLength)

	t.index++

	return C.GoStringN(content, C.int(contentLength)), wrapError(err, "ts_bulk_get_string")
}

// RowSetString : Set string at specified index in current row
func (t *TsBatch) RowSetString(index int64, content string) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)

	return wrapError(C.qdb_ts_batch_row_set_string(t.table, valueIndex, contentPtr, contentSize), "ts_batch_row_set_string", "index", valueIndex, "value_size", len(content))
}

// RowSetStringNoCopy : Set string at specified index in current row without copying it
func (t *TsBatch) RowSetStringNoCopy(index int64, content string) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)

	return wrapError(C.qdb_ts_batch_row_set_string_no_copy(t.table, valueIndex, contentPtr, contentSize), "ts_batch_row_set_string_no_copy", "index", valueIndex, "value_size", len(content))
}
