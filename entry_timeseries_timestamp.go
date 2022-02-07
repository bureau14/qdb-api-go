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

// TsTimestampPoint : timestamped timestamp data point
type TsTimestampPoint struct {
	timestamp time.Time
	content   time.Time
}

// Timestamp : return data point timestamp
func (t TsTimestampPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsTimestampPoint) Content() time.Time {
	return t.content
}

// NewTsTimestampPoint : Create new timeseries timestamp point
func NewTsTimestampPoint(timestamp time.Time, value time.Time) TsTimestampPoint {
	return TsTimestampPoint{timestamp, value}
}

// :: internals
func (t TsTimestampPoint) toStructC() C.qdb_ts_timestamp_point {
	return C.qdb_ts_timestamp_point{toQdbTimespec(t.timestamp), toQdbTimespec(t.content)}
}

func (t C.qdb_ts_timestamp_point) toStructG() TsTimestampPoint {
	return TsTimestampPoint{t.timestamp.toStructG(), t.value.toStructG()}
}

func timestampPointArrayToC(pts ...TsTimestampPoint) *C.qdb_ts_timestamp_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_timestamp_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func timestampPointArrayToSlice(points *C.qdb_ts_timestamp_point, length int) []C.qdb_ts_timestamp_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_timestamp_point{})]C.qdb_ts_timestamp_point)(unsafe.Pointer(points))[:length:length]
}

func timestampPointArrayToGo(points *C.qdb_ts_timestamp_point, pointsCount C.qdb_size_t) []TsTimestampPoint {
	length := int(pointsCount)
	output := make([]TsTimestampPoint, length)
	if length > 0 {
		slice := timestampPointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// TsTimestampColumn : a time series timestamp column
type TsTimestampColumn struct {
	tsColumn
}

// TimestampColumn : create a column object
func (entry TimeseriesEntry) TimestampColumn(columnName string) TsTimestampColumn {
	return TsTimestampColumn{tsColumn{NewTsColumnInfo(columnName, TsColumnTimestamp), entry}}
}

// Insert timestamp points into a timeseries
func (column TsTimestampColumn) Insert(points ...TsTimestampPoint) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := timestampPointArrayToC(points...)
	err := C.qdb_ts_timestamp_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// EraseRanges : erase all points in the specified ranges
func (column TsTimestampColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
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

// GetRanges : Retrieves timestamps in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (column TsTimestampColumn) GetRanges(rgs ...TsRange) ([]TsTimestampPoint, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_timestamp_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_timestamp_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return timestampPointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

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
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_timestamp_aggregation_t) toStructG() TsTimestampAggregation {
	var gAgg TsTimestampAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t._range.toStructG()
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

func timestampAggregationArrayToSlice(aggregations *C.qdb_ts_timestamp_aggregation_t, length int) []C.qdb_ts_timestamp_aggregation_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_timestamp_aggregation_t{})]C.qdb_ts_timestamp_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
}

func timestampAggregationArrayToGo(aggregations *C.qdb_ts_timestamp_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsTimestampAggregation) []TsTimestampAggregation {
	length := int(aggregationsCount)
	output := make([]TsTimestampAggregation, length)
	if length > 0 {
		slice := timestampAggregationArrayToSlice(aggregations, length)
		for i, s := range slice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// TODO(Vianney): Implement aggregate

// Aggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
//	It is an error to call this function on a non existing time-series.
func (column TsTimestampColumn) Aggregate(aggs ...*TsTimestampAggregation) ([]TsTimestampAggregation, error) {
	return nil, ErrNotImplemented
}

// GetTimestamp : gets a timestamp in row
func (t *TsBulk) GetTimestamp() (time.Time, error) {
	var content C.qdb_timespec_t
	err := C.qdb_ts_row_get_timestamp(t.table, C.qdb_size_t(t.index), &content)
	t.index++
	return content.toStructG(), makeErrorOrNil(err)
}

// RowSetTimestamp : Add a timestamp to current row
func (t *TsBatch) RowSetTimestamp(index int64, value time.Time) error {
	valueIndex := C.qdb_size_t(index)
	cValue := toQdbTimespec(value)
	return makeErrorOrNil(C.qdb_ts_batch_row_set_timestamp(t.table, valueIndex, &cValue))
}
