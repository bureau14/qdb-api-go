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

// TsColumnType : Timeseries column types
type TsColumnType C.qdb_ts_column_type_t

// Values
//	TsColumnDouble : column is a double point
//	TsColumnBlob : column is a blob point
//	TsColumnInt64 : column is a int64 point
//	TsColumnTimestamp : column is a timestamp point
//	TsColumnString : column is a string point
//	TsColumnSymbol : column is a symbol point
const (
	TsColumnUninitialized TsColumnType = C.qdb_ts_column_uninitialized
	TsColumnBlob          TsColumnType = C.qdb_ts_column_blob
	TsColumnDouble        TsColumnType = C.qdb_ts_column_double
	TsColumnInt64         TsColumnType = C.qdb_ts_column_int64
	TsColumnString        TsColumnType = C.qdb_ts_column_string
	TsColumnTimestamp     TsColumnType = C.qdb_ts_column_timestamp
	TsColumnSymbol        TsColumnType = C.qdb_ts_column_symbol
)

type tsColumn struct {
	TsColumnInfo
	parent TimeseriesEntry
}

// TsColumnInfo : column information in timeseries
type TsColumnInfo struct {
	name     string
	kind     TsColumnType
	symtable string
}

// Name : return column name
func (t TsColumnInfo) Name() string {
	return t.name
}

// Type : return column type
func (t TsColumnInfo) Type() TsColumnType {
	return t.kind
}

// Symtable : return column symbol table name
func (t TsColumnInfo) Symtable() string {
	return t.symtable
}

// NewTsColumnInfo : create a column info structure
func NewTsColumnInfo(columnName string, columnType TsColumnType) TsColumnInfo {
	return TsColumnInfo{columnName, columnType, ""}
}
func NewSymbolColumnInfo(columnName string, symtableName string) TsColumnInfo {
	return TsColumnInfo{columnName, TsColumnSymbol, symtableName}
}

// :: internals
func (t TsColumnInfo) toStructC() C.qdb_ts_column_info_ex_t {
	return C.qdb_ts_column_info_ex_t{name: convertToCharStar(t.name), _type: C.qdb_ts_column_type_t(t.kind), symtable: convertToCharStar(t.symtable)}
}

func (t C.qdb_ts_column_info_ex_t) toStructInfoG() TsColumnInfo {
	return TsColumnInfo{C.GoString(t.name), TsColumnType(t._type), C.GoString(t.symtable)}
}

func columnInfoArrayToC(cols ...TsColumnInfo) *C.qdb_ts_column_info_ex_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_column_info_ex_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return &columns[0]
}

func oldColumnInfoArrayToC(cols ...TsColumnInfo) *C.qdb_ts_column_info_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_column_info_ex_t, len(cols))
	for idx, col := range cols {
		columns[idx] = C.qdb_ts_column_info_ex_t{name: convertToCharStar(col.name), _type: C.qdb_ts_column_type_t(col.kind)}
	}
	return &columns[0]
}

func releaseColumnInfoArray(columns *C.qdb_ts_column_info_ex_t, length int) {
	if length > 0 {
		slice := columnInfoArrayToSlice(columns, length)
		for _, s := range slice {
			releaseCharStar(s.name)
			releaseCharStar(s.symtable)
		}
	}
}

func releaseOldColumnInfoArray(columns *C.qdb_ts_column_info_t, length int) {
	if length > 0 {
		slice := columnInfoArrayToSlice(columns, length)
		for _, s := range slice {
			releaseCharStar(s.name)
		}
	}
}

func columnInfoArrayToGo(columns *C.qdb_ts_column_info_ex_t, columnsCount C.qdb_size_t) []TsColumnInfo {
	length := int(columnsCount)
	columnsInfo := make([]TsColumnInfo, length)
	if length > 0 {
		slice := columnInfoArrayToSlice(columns, length)
		for i, s := range slice {
			columnsInfo[i] = s.toStructInfoG()
		}
	}
	return columnsInfo
}

// TimeseriesEntry : timeseries double entry data type
type TimeseriesEntry struct {
	Entry
}

// :: internals

func (t C.qdb_ts_column_info_ex_t) toStructG(entry TimeseriesEntry) tsColumn {
	return tsColumn{TsColumnInfo{C.GoString(t.name), TsColumnType(t._type), C.GoString(t.symtable)}, entry}
}

func columnInfoArrayToSlice(columns *C.qdb_ts_column_info_ex_t, length int) []C.qdb_ts_column_info_ex_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_column_info_ex_t{})]C.qdb_ts_column_info_ex_t)(unsafe.Pointer(columns))[:length:length]
}

func columnArrayToGo(entry TimeseriesEntry, columns *C.qdb_ts_column_info_ex_t, columnsCount C.qdb_size_t) ([]TsBlobColumn, []TsDoubleColumn, []TsInt64Column, []TsStringColumn, []TsTimestampColumn, []TsSymbolColumn) {
	length := int(columnsCount)
	blobColumns := []TsBlobColumn{}
	doubleColumns := []TsDoubleColumn{}
	int64Columns := []TsInt64Column{}
	stringColumns := []TsStringColumn{}
	timestampColumns := []TsTimestampColumn{}
	symbolColumns := []TsSymbolColumn{}
	if length > 0 {
		slice := columnInfoArrayToSlice(columns, length)
		for _, s := range slice {
			if s._type == C.qdb_ts_column_blob {
				blobColumns = append(blobColumns, TsBlobColumn{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_double {
				doubleColumns = append(doubleColumns, TsDoubleColumn{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_int64 {
				int64Columns = append(int64Columns, TsInt64Column{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_string {
				stringColumns = append(stringColumns, TsStringColumn{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_timestamp {
				timestampColumns = append(timestampColumns, TsTimestampColumn{s.toStructG(entry)})
			} else if s._type == C.qdb_ts_column_symbol {
				symbolColumns = append(symbolColumns, TsSymbolColumn{s.toStructG(entry)})
			}
		}
	}
	return blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, symbolColumns
}

// Columns : return the current columns
func (entry TimeseriesEntry) Columns() ([]TsBlobColumn, []TsDoubleColumn, []TsInt64Column, []TsStringColumn, []TsTimestampColumn, []TsSymbolColumn, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var columns *C.qdb_ts_column_info_ex_t
	var columnsCount C.qdb_size_t
	err := C.qdb_ts_list_columns_ex(entry.handle, alias, &columns, &columnsCount)
	var blobColumns []TsBlobColumn
	var doubleColumns []TsDoubleColumn
	var int64Columns []TsInt64Column
	var stringColumns []TsStringColumn
	var timestampColumns []TsTimestampColumn
	var symbolColumns []TsSymbolColumn
	if err == 0 {
		blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, symbolColumns = columnArrayToGo(entry, columns, columnsCount)
	}
	return blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, symbolColumns, makeErrorOrNil(err)
}

// ColumnsInfo : return the current columns information
func (entry TimeseriesEntry) ColumnsInfo() ([]TsColumnInfo, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var columns *C.qdb_ts_column_info_ex_t
	var columnsCount C.qdb_size_t
	err := C.qdb_ts_list_columns_ex(entry.handle, alias, &columns, &columnsCount)
	var columnsInfo []TsColumnInfo
	if err == 0 {
		columnsInfo = columnInfoArrayToGo(columns, columnsCount)
	}
	return columnsInfo, makeErrorOrNil(err)
}

// Create : create a new timeseries
//	First parameter is the duration limit to organize a shard
//	Ex: shardSize := 24 * time.Hour
func (entry TimeseriesEntry) Create(shardSize time.Duration, cols ...TsColumnInfo) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	duration := C.qdb_uint_t(shardSize / time.Millisecond)
	columns := columnInfoArrayToC(cols...)
	defer releaseColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_create_ex(entry.handle, alias, duration, columns, columnsCount)
	return makeErrorOrNil(err)
}

// InsertColumns : insert columns in a existing timeseries
func (entry TimeseriesEntry) InsertColumns(cols ...TsColumnInfo) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	columns := columnInfoArrayToC(cols...)
	defer releaseColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_insert_columns_ex(entry.handle, alias, columns, columnsCount)
	return makeErrorOrNil(err)
}

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

// NewRange : creates a time range
func NewRange(begin, end time.Time) TsRange {
	return TsRange{begin: begin, end: end}
}

// :: internals
func (t TsRange) toStructC() C.qdb_ts_range_t {
	r := C.qdb_ts_range_t{begin: toQdbTimespec(t.begin), end: toQdbTimespec(t.end)}
	return r
}

func (t C.qdb_ts_range_t) toStructG() TsRange {
	r := NewRange(t.begin.toStructG(), t.end.toStructG())
	return r
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

// TsBulk : A structure that permits to append data to a timeseries
type TsBulk struct {
	h         HandleType
	err       error
	rowCount  int
	index     int
	timestamp time.Time
	table     C.qdb_local_table_t
}

// Bulk : create a bulk object for the specified columns
//	If no columns are specified it gets the server side registered columns
func (entry TimeseriesEntry) Bulk(cols ...TsColumnInfo) (*TsBulk, error) {
	if len(cols) == 0 {
		var err error
		cols, err = entry.ColumnsInfo()
		if err != nil {
			return nil, err
		}
	}
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	columns := oldColumnInfoArrayToC(cols...)
	defer releaseOldColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	bulk := &TsBulk{}
	bulk.h = entry.HandleType
	err := C.qdb_ts_local_table_init(entry.handle, alias, columns, columnsCount, &bulk.table)
	return bulk, makeErrorOrNil(err)
}

// Row : initialize a row append
func (t *TsBulk) Row(timestamp time.Time) *TsBulk {
	t.timestamp = timestamp
	t.index = 0
	return t
}

// RowCount : returns the number of rows to be append
func (t TsBulk) RowCount() int {
	return t.rowCount
}

// Ignore : ignores this column in a row transaction
func (t *TsBulk) Ignore() *TsBulk {
	t.index++
	return t
}

// Append : Adds the append to the list to be pushed
func (t *TsBulk) Append() error {
	if t.err != nil {
		return t.err
	}
	rowIndex := C.qdb_size_t(0)
	timespec := toQdbTimespec(t.timestamp)
	err := C.qdb_ts_table_row_append(t.table, &timespec, &rowIndex)
	if err == 0 {
		t.rowCount = int(rowIndex) + 1
	}
	t.timestamp = time.Unix(0, 0)
	return makeErrorOrNil(err)
}

// Push : push the list of appended rows
// returns the number of rows added
func (t *TsBulk) Push() (int, error) {
	err := C.qdb_ts_push(t.table)
	rowCount := t.rowCount
	t.rowCount = 0
	return rowCount, makeErrorOrNil(err)
}

// GetRanges : create a range bulk query
func (t *TsBulk) GetRanges(rgs ...TsRange) error {
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	err := C.qdb_ts_table_stream_ranges(t.table, ranges, rangesCount)
	t.rowCount = -1
	t.index = 0
	return makeErrorOrNil(err)
}

// NextRow : advance to the next row, or the first one if not already used
func (t *TsBulk) NextRow() (time.Time, error) {
	var timestamp C.qdb_timespec_t
	err := C.qdb_ts_table_next_row(t.table, &timestamp)
	t.rowCount++
	t.index = 0
	return timestamp.toStructG(), makeErrorOrNil(err)
}

// Release : release the memory of the local table
func (t *TsBulk) Release() {
	t.h.Release(unsafe.Pointer(t.table))
}

// TsBatch : A structure that permits to append data to a timeseries
type TsBatch struct {
	h     HandleType
	err   error
	table C.qdb_batch_table_t
}

// TsBatchColumnInfo : Represents one column in a timeseries
// Preallocate the underlying structure with the ElementCountHint
type TsBatchColumnInfo struct {
	Timeseries       string
	Column           string
	ElementCountHint int64
}

// :: internals
func (t TsBatchColumnInfo) toStructC() C.qdb_ts_batch_column_info_t {
	return C.qdb_ts_batch_column_info_t{convertToCharStar(t.Timeseries), convertToCharStar(t.Column), C.qdb_size_t(t.ElementCountHint)}
}

func (t C.qdb_ts_batch_column_info_t) toStructInfoG() TsBatchColumnInfo {
	return TsBatchColumnInfo{C.GoString(t.timeseries), C.GoString(t.column), int64(t.elements_count_hint)}
}
func tsBatchColumnInfoArrayToC(cols ...TsBatchColumnInfo) *C.qdb_ts_batch_column_info_t {
	if len(cols) == 0 {
		return nil
	}
	columns := make([]C.qdb_ts_batch_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = col.toStructC()
	}
	return &columns[0]
}

func batchColumnInfoArrayToSlice(columns *C.qdb_ts_batch_column_info_t, length int) []C.qdb_ts_batch_column_info_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_batch_column_info_t{})]C.qdb_ts_batch_column_info_t)(unsafe.Pointer(columns))[:length:length]
}

func releaseTsBatchColumnInfoArray(columns *C.qdb_ts_batch_column_info_t, length int) {
	if length > 0 {
		slice := batchColumnInfoArrayToSlice(columns, length)
		for _, s := range slice {
			releaseCharStar(s.timeseries)
			releaseCharStar(s.column)
		}
	}
}

func tsBatchColumnInfoArrayToGo(columns *C.qdb_ts_batch_column_info_t, columnsCount C.qdb_size_t) []TsBatchColumnInfo {
	length := int(columnsCount)
	columnsInfo := make([]TsBatchColumnInfo, length)
	if length > 0 {
		slice := batchColumnInfoArrayToSlice(columns, length)
		for i, s := range slice {
			columnsInfo[i] = s.toStructInfoG()
		}
	}
	return columnsInfo
}

// NewTsBatchColumnInfo : Creates a new TsBatchColumnInfo
func NewTsBatchColumnInfo(timeseries string, column string, hint int64) TsBatchColumnInfo {
	return TsBatchColumnInfo{timeseries, column, hint}
}

// ExtraColumns : Appends columns to the current batch table
func (t *TsBatch) ExtraColumns(cols ...TsBatchColumnInfo) error {
	columns := tsBatchColumnInfoArrayToC(cols...)
	defer releaseTsBatchColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_batch_table_extra_columns(t.table, columns, columnsCount)
	return makeErrorOrNil(err)
}

// StartRow : Start a new row
func (t *TsBatch) StartRow(timestamp time.Time) error {
	cTimestamp := toQdbTimespec(timestamp)
	return makeErrorOrNil(C.qdb_ts_batch_start_row(t.table, &cTimestamp))
}

// Push : Push the inserted data
func (t *TsBatch) Push() error {
	return makeErrorOrNil(C.qdb_ts_batch_push(t.table))
}

// PushFast : Fast, in-place batch push that is efficient when doing lots of small, incremental pushes.
func (t *TsBatch) PushFast() error {
	return makeErrorOrNil(C.qdb_ts_batch_push_fast(t.table))
}

// Release : release the memory of the batch table
func (t *TsBatch) Release() {
	t.h.Release(unsafe.Pointer(t.table))
}
