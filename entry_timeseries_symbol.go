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

// TsSymbolPoint : timestamped symbol
type TsSymbolPoint struct {
	timestamp time.Time
	content   string
}

// Timestamp : return data point timestamp
func (t TsSymbolPoint) Timestamp() time.Time {
	return t.timestamp
}

// Content : return data point content
func (t TsSymbolPoint) Content() string {
	return t.content
}

// NewTsSymbolPoint : Create new timeseries symbol point
func NewTsSymbolPoint(timestamp time.Time, value string) TsSymbolPoint {
	return TsSymbolPoint{timestamp, value}
}

// :: internals
func (t TsSymbolPoint) toStructC() C.qdb_ts_symbol_point {
	dataSize := C.qdb_size_t(len(t.content))
	data := convertToCharStar(string(t.content))
	return C.qdb_ts_symbol_point{toQdbTimespec(t.timestamp), data, dataSize}
}

func (t C.qdb_ts_symbol_point) toStructG() TsSymbolPoint {
	return TsSymbolPoint{t.timestamp.toStructG(), C.GoStringN(t.content, C.int(t.content_length))}
}

func symbolPointArrayToC(pts ...TsSymbolPoint) *C.qdb_ts_symbol_point {
	if len(pts) == 0 {
		return nil
	}
	points := make([]C.qdb_ts_symbol_point, len(pts))
	for idx, pt := range pts {
		points[idx] = pt.toStructC()
	}
	return &points[0]
}

func releaseSymbolPointArray(points *C.qdb_ts_symbol_point, length int) {
	if length > 0 {
		slice := symbolPointArrayToSlice(points, length)
		for _, s := range slice {
			C.free(unsafe.Pointer(s.content))
		}
	}
}

func symbolPointArrayToSlice(points *C.qdb_ts_symbol_point, length int) []C.qdb_ts_symbol_point {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_symbol_point{})]C.qdb_ts_symbol_point)(unsafe.Pointer(points))[:length:length]
}

func symbolPointArrayToGo(points *C.qdb_ts_symbol_point, pointsCount C.qdb_size_t) []TsSymbolPoint {
	length := int(pointsCount)
	output := make([]TsSymbolPoint, length)
	if length > 0 {
		slice := symbolPointArrayToSlice(points, length)
		for i, s := range slice {
			output[i] = s.toStructG()
		}
	}
	return output
}

// TsSymbolColumn : a time series symbol column
type TsSymbolColumn struct {
	tsColumn
}

// SymbolColumn : create a column object (the symbol table name is not set)
func (entry TimeseriesEntry) SymbolColumn(columnName string, symtableName string) TsSymbolColumn {
	return TsSymbolColumn{tsColumn{TsColumnInfo{columnName, TsColumnSymbol, symtableName}, entry}}
}

// Insert symbol points into a timeseries
func (column TsSymbolColumn) Insert(points ...TsSymbolPoint) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := symbolPointArrayToC(points...)
	defer releaseSymbolPointArray(content, len(points))
	err := C.qdb_ts_symbol_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
}

// EraseRanges : erase all points in the specified ranges
func (column TsSymbolColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
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

// GetRanges : Retrieves symbols in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (column TsSymbolColumn) GetRanges(rgs ...TsRange) ([]TsSymbolPoint, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_symbol_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_symbol_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return symbolPointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// TsSymbolAggregation : Aggregation of double type
type TsSymbolAggregation struct {
	kind  TsAggregationType
	rng   TsRange
	count int64
	point TsSymbolPoint
}

// Type : returns the type of the aggregation
func (t TsSymbolAggregation) Type() TsAggregationType {
	return t.kind
}

// Range : returns the range of the aggregation
func (t TsSymbolAggregation) Range() TsRange {
	return t.rng
}

// Count : returns the number of points aggregated into the result
func (t TsSymbolAggregation) Count() int64 {
	return t.count
}

// Result : result of the aggregation
func (t TsSymbolAggregation) Result() TsSymbolPoint {
	return t.point
}

// NewSymbolAggregation : Create new timeseries string aggregation
func NewSymbolAggregation(kind TsAggregationType, rng TsRange) *TsSymbolAggregation {
	return &TsSymbolAggregation{kind, rng, 0, TsSymbolPoint{}}
}

// :: internals
func (t TsSymbolAggregation) toStructC() C.qdb_ts_symbol_aggregation_t {
	var cAgg C.qdb_ts_symbol_aggregation_t
	cAgg._type = C.qdb_ts_aggregation_type_t(t.kind)
	cAgg._range = t.rng.toStructC()
	cAgg.count = C.qdb_size_t(t.count)
	cAgg.result = t.point.toStructC()
	return cAgg
}

func (t C.qdb_ts_symbol_aggregation_t) toStructG() TsSymbolAggregation {
	var gAgg TsSymbolAggregation
	gAgg.kind = TsAggregationType(t._type)
	gAgg.rng = t._range.toStructG()
	gAgg.count = int64(t.count)
	gAgg.point = t.result.toStructG()
	return gAgg
}

func symbolAggregationArrayToC(ags ...*TsSymbolAggregation) *C.qdb_ts_symbol_aggregation_t {
	if len(ags) == 0 {
		return nil
	}
	var symbolAggregations []C.qdb_ts_symbol_aggregation_t
	for _, ag := range ags {
		symbolAggregations = append(symbolAggregations, ag.toStructC())
	}
	return &symbolAggregations[0]
}

func symbolAggregationArrayToSlice(aggregations *C.qdb_ts_symbol_aggregation_t, length int) []C.qdb_ts_symbol_aggregation_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_symbol_aggregation_t{})]C.qdb_ts_symbol_aggregation_t)(unsafe.Pointer(aggregations))[:length:length]
}

func symbolAggregationArrayToGo(aggregations *C.qdb_ts_symbol_aggregation_t, aggregationsCount C.qdb_size_t, aggs []*TsSymbolAggregation) []TsSymbolAggregation {
	length := int(aggregationsCount)
	output := make([]TsSymbolAggregation, length)
	if length > 0 {
		slice := symbolAggregationArrayToSlice(aggregations, length)
		for i, s := range slice {
			*aggs[i] = s.toStructG()
			output[i] = s.toStructG()
		}
	}
	return output
}

// Aggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (column TsSymbolColumn) Aggregate(aggs ...*TsSymbolAggregation) ([]TsSymbolAggregation, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	aggregations := symbolAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsSymbolAggregation
	err := C.qdb_ts_symbol_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = symbolAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
}

// Symbol : adds a symbol in row transaction
func (t *TsBulk) Symbol(content string) *TsBulk {
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)
	if t.err == nil {
		t.err = makeErrorOrNil(C.qdb_ts_row_set_symbol(t.table, C.qdb_size_t(t.index), contentPtr, contentSize))
	}
	t.index++
	return t
}

// GetSymbol : gets a symbol in row
func (t *TsBulk) GetSymbol() (string, error) {
	var content *C.char
	defer t.h.Release(unsafe.Pointer(content))
	var contentLength C.qdb_size_t
	err := C.qdb_ts_row_get_symbol(t.table, C.qdb_size_t(t.index), &content, &contentLength)

	t.index++
	return C.GoStringN(content, C.int(contentLength)), makeErrorOrNil(err)
}

// RowSetSymbol : Set symbol at specified index in current row
func (t *TsBatch) RowSetSymbol(index int64, content string) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)
	return makeErrorOrNil(C.qdb_ts_batch_row_set_symbol(t.table, valueIndex, contentPtr, contentSize))
}

// RowSetSymbolNoCopy : Set symbol at specified index in current row without copying it
func (t *TsBatch) RowSetSymbolNoCopy(index int64, content string) error {
	valueIndex := C.qdb_size_t(index)
	contentSize := C.qdb_size_t(len(content))
	contentPtr := convertToCharStar(content)
	defer releaseCharStar(contentPtr)
	return makeErrorOrNil(C.qdb_ts_batch_row_set_symbol_no_copy(t.table, valueIndex, contentPtr, contentSize))
}
