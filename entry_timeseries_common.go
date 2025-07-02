package qdb

/*
	#include <qdb/ts.h>
	#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"math"
	"runtime"
	"time"
	"unsafe"
)

// TsColumnType : Timeseries column types
type TsColumnType C.qdb_ts_column_type_t

// Values
//
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

var TsColumnTypes = []TsColumnType{
	TsColumnBlob,
	TsColumnDouble,
	TsColumnInt64,
	TsColumnString,
	TsColumnTimestamp,
	TsColumnSymbol,
}

// TsValueType : Timeseries value types
//
// Values we're able to represent inside a database, as some values are represented
// differently as columns. A good example are Symbol columns, where the user interacts
// with the values as strings, but on-disk are stored as an indexed integer.
type TsValueType int

const (
	TsValueNull TsValueType = iota

	TsValueDouble
	TsValueInt64
	TsValueTimestamp
	TsValueBlob
	TsValueString
)

var TsValueTypes = []TsValueType{
	TsValueBlob,
	TsValueDouble,
	TsValueInt64,
	TsValueString,
	TsValueTimestamp,
}

func (v TsValueType) AsColumnType() TsColumnType {
	switch v {
	case TsValueBlob:
		return TsColumnBlob
	case TsValueString:
		// Can be either String or Symbol, but we'll default to Symbols. This mostly affects
		// "magic" table creation.
		return TsColumnString
	case TsValueDouble:
		return TsColumnDouble
	case TsValueInt64:
		return TsColumnInt64
	case TsValueNull:
		break
	}

	panic(fmt.Sprintf("Unrecognized value type: %v", v))
}

// Returns true if this column is valid and non-null
func (v TsColumnType) IsValid() bool {
	switch v {
	case TsColumnBlob:
		fallthrough
	case TsColumnSymbol:
		fallthrough
	case TsColumnString:
		fallthrough
	case TsColumnDouble:
		fallthrough
	case TsColumnInt64:
		fallthrough
	case TsColumnTimestamp:
		return true
	}

	return false
}

func (v TsColumnType) AsValueType() TsValueType {
	switch v {
	case TsColumnBlob:
		return TsValueBlob
	case TsColumnSymbol:
		// Both strings and symbols are represented as string values client-side
		fallthrough
	case TsColumnString:
		return TsValueString
	case TsColumnDouble:
		return TsValueDouble
	case TsColumnInt64:
		return TsValueInt64
	case TsColumnTimestamp:
		return TsValueTimestamp
	}

	panic(fmt.Sprintf("Unrecognized column type: %v", v))
}

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

func TsColumnInfoExToStructInfoG(t C.qdb_ts_column_info_ex_t) TsColumnInfo {
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
	columns := make([]C.qdb_ts_column_info_t, len(cols))
	for idx, col := range cols {
		columns[idx] = C.qdb_ts_column_info_t{name: convertToCharStar(col.name), _type: C.qdb_ts_column_type_t(col.kind)}
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
		slice := oldColumnInfoArrayToSlice(columns, length)
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
			columnsInfo[i] = TsColumnInfoExToStructInfoG(s)
		}
	}
	return columnsInfo
}

// TimeseriesEntry : timeseries double entry data type
type TimeseriesEntry struct {
	Entry
}

// Name : Returns the name of the table
func (t TimeseriesEntry) Name() string {
	return t.Entry.Alias()
}

// :: internals

func TsColumnInfoExToStructG(t C.qdb_ts_column_info_ex_t, entry TimeseriesEntry) tsColumn {
	return tsColumn{TsColumnInfo{C.GoString(t.name), TsColumnType(t._type), C.GoString(t.symtable)}, entry}
}

func columnInfoArrayToSlice(columns *C.qdb_ts_column_info_ex_t, length int) []C.qdb_ts_column_info_ex_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_column_info_ex_t{})]C.qdb_ts_column_info_ex_t)(unsafe.Pointer(columns))[:length:length]
}

func oldColumnInfoArrayToSlice(columns *C.qdb_ts_column_info_t, length int) []C.qdb_ts_column_info_t {
	// See https://github.com/mattn/go-sqlite3/issues/238 for details.
	return (*[(math.MaxInt32 - 1) / unsafe.Sizeof(C.qdb_ts_column_info_t{})]C.qdb_ts_column_info_t)(unsafe.Pointer(columns))[:length:length]
}

func columnArrayToGo(entry TimeseriesEntry, columns *C.qdb_ts_column_info_ex_t, columnsCount C.qdb_size_t) ([]TsBlobColumn, []TsDoubleColumn, []TsInt64Column, []TsStringColumn, []TsTimestampColumn) {
	length := int(columnsCount)
	blobColumns := []TsBlobColumn{}
	doubleColumns := []TsDoubleColumn{}
	int64Columns := []TsInt64Column{}
	stringColumns := []TsStringColumn{}
	timestampColumns := []TsTimestampColumn{}
	if length > 0 {
		slice := columnInfoArrayToSlice(columns, length)
		for _, s := range slice {
			if s._type == C.qdb_ts_column_blob {
				blobColumns = append(blobColumns, TsBlobColumn{TsColumnInfoExToStructG(s, entry)})
			} else if s._type == C.qdb_ts_column_double {
				doubleColumns = append(doubleColumns, TsDoubleColumn{TsColumnInfoExToStructG(s, entry)})
			} else if s._type == C.qdb_ts_column_int64 {
				int64Columns = append(int64Columns, TsInt64Column{TsColumnInfoExToStructG(s, entry)})
			} else if s._type == C.qdb_ts_column_string || s._type == C.qdb_ts_column_symbol {
				stringColumns = append(stringColumns, TsStringColumn{TsColumnInfoExToStructG(s, entry)})
			} else if s._type == C.qdb_ts_column_timestamp {
				timestampColumns = append(timestampColumns, TsTimestampColumn{TsColumnInfoExToStructG(s, entry)})
			}
		}
	}
	return blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns
}

// Columns : return the current columns
func (entry TimeseriesEntry) Columns() ([]TsBlobColumn, []TsDoubleColumn, []TsInt64Column, []TsStringColumn, []TsTimestampColumn, error) {
	var p runtime.Pinner

	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)

	var metadata *C.qdb_ts_metadata_t
	p.Pin(&metadata)
	err := C.qdb_ts_get_metadata(entry.handle, alias, &metadata)
	p.Unpin()

	if metadata != nil {
		defer C.qdb_release(entry.handle, unsafe.Pointer(metadata))
	}

	var blobColumns []TsBlobColumn
	var doubleColumns []TsDoubleColumn
	var int64Columns []TsInt64Column
	var stringColumns []TsStringColumn
	var timestampColumns []TsTimestampColumn
	if err == 0 {
		blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns = columnArrayToGo(entry, metadata.columns, metadata.column_count)
	}
	return blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, makeErrorOrNil(err)
}

// ColumnsInfo : return the current columns information
func (entry TimeseriesEntry) ColumnsInfo() ([]TsColumnInfo, error) {
	var p runtime.Pinner

	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var metadata *C.qdb_ts_metadata_t
	p.Pin(&metadata)
	err := C.qdb_ts_get_metadata(entry.handle, alias, &metadata)
	p.Unpin()

	if metadata != nil {
		defer C.qdb_release(entry.handle, unsafe.Pointer(metadata))
	}

	var columnsInfo []TsColumnInfo
	if err == 0 {
		columnsInfo = columnInfoArrayToGo(metadata.columns, metadata.column_count)
	}
	return columnsInfo, makeErrorOrNil(err)
}

// Create : create a new timeseries
//
//	First parameter is the duration limit to organize a shard
//	Ex: shardSize := 24 * time.Hour
func (entry TimeseriesEntry) Create(shardSize time.Duration, cols ...TsColumnInfo) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	duration := C.qdb_uint_t(shardSize / time.Millisecond)
	columns := columnInfoArrayToC(cols...)
	defer releaseColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_create_ex(entry.handle, alias, duration, columns, columnsCount, C.qdb_never_expires)
	if err == C.qdb_e_ok {
		L().Debug("successfully created table", "name", entry.alias, "shard_size", shardSize)
	}
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

func TsRangeToStructG(t C.qdb_ts_range_t) TsRange {
	r := NewRange(TimespecToStructG(t.begin), TimespecToStructG(t.end))
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
//
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
	return TimespecToStructG(timestamp), makeErrorOrNil(err)
}

// Release : release the memory of the local table
func (t *TsBulk) Release() {
	t.h.Release(unsafe.Pointer(t.table))
}

// TsBatch represents a batch writer for efficient bulk insertion into timeseries tables.
//
// Batch operations significantly improve performance when inserting large amounts of data.
// All columns must be specified at initialization and cannot be changed afterward.
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

func TsBatchColumnInfoToStructInfoG(t C.qdb_ts_batch_column_info_t) TsBatchColumnInfo {
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
			columnsInfo[i] = TsBatchColumnInfoToStructInfoG(s)
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
