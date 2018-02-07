package qdb

/*
	#include <qdb/ts.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// TimeseriesEntry : timeseries double entry data type
type TimeseriesEntry struct {
	Entry
}

// Columns : return the current columns
func (entry TimeseriesEntry) Columns() ([]TsDoubleColumn, []TsBlobColumn, []TsInt64Column, []TsTimestampColumn, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var columns *C.qdb_ts_column_info_t
	var columnsCount C.qdb_size_t
	err := C.qdb_ts_list_columns(entry.handle, alias, &columns, &columnsCount)
	var doubleColumns []TsDoubleColumn
	var blobColumns []TsBlobColumn
	var int64Columns []TsInt64Column
	var timestampColumns []TsTimestampColumn
	if err == 0 {
		doubleColumns, blobColumns, int64Columns, timestampColumns = columnArrayToGo(entry, columns, columnsCount)
	}
	return doubleColumns, blobColumns, int64Columns, timestampColumns, makeErrorOrNil(err)
}

// ColumnsInfo : return the current columns information
func (entry TimeseriesEntry) ColumnsInfo() ([]TsColumnInfo, error) {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	var columns *C.qdb_ts_column_info_t
	var columnsCount C.qdb_size_t
	err := C.qdb_ts_list_columns(entry.handle, alias, &columns, &columnsCount)
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
	err := C.qdb_ts_create(entry.handle, alias, duration, columns, columnsCount)
	return makeErrorOrNil(err)
}

// InsertColumns : insert columns in a existing timeseries
func (entry TimeseriesEntry) InsertColumns(cols ...TsColumnInfo) error {
	alias := convertToCharStar(entry.alias)
	defer releaseCharStar(alias)
	columns := columnInfoArrayToC(cols...)
	defer releaseColumnInfoArray(columns, len(cols))
	columnsCount := C.qdb_size_t(len(cols))
	err := C.qdb_ts_insert_columns(entry.handle, alias, columns, columnsCount)
	return makeErrorOrNil(err)
}

// DoubleColumn : create a column object
func (entry TimeseriesEntry) DoubleColumn(columnName string) TsDoubleColumn {
	return TsDoubleColumn{tsColumn{TsColumnInfo{columnName, TsColumnDouble}, entry}}
}

// BlobColumn : create a column object
func (entry TimeseriesEntry) BlobColumn(columnName string) TsBlobColumn {
	return TsBlobColumn{tsColumn{TsColumnInfo{columnName, TsColumnBlob}, entry}}
}

// Int64Column : create a column object
func (entry TimeseriesEntry) Int64Column(columnName string) TsInt64Column {
	return TsInt64Column{tsColumn{TsColumnInfo{columnName, TsColumnInt64}, entry}}
}

// TimestampColumn : create a column object
func (entry TimeseriesEntry) TimestampColumn(columnName string) TsTimestampColumn {
	return TsTimestampColumn{tsColumn{TsColumnInfo{columnName, TsColumnTimestamp}, entry}}
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

// EraseRanges : erase all points in the specified ranges
func (column TsBlobColumn) EraseRanges(rgs ...TsRange) (uint64, error) {
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
	return uint64(erasedCount), makeErrorOrNil(err)
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

// Insert blob points into a timeseries
func (column TsBlobColumn) Insert(points ...TsBlobPoint) error {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	contentCount := C.qdb_size_t(len(points))
	content := blobPointArrayToC(points...)
	defer releaseBlobPointArray(content, len(points))
	err := C.qdb_ts_blob_insert(column.parent.handle, alias, columnName, content, contentCount)
	return makeErrorOrNil(err)
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
	return makeErrorOrNil(err)
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

// GetRanges : Retrieves blobs in the specified range of the time series column.
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

// GetRanges : Retrieves blobs in the specified range of the time series column.
//	It is an error to call this function on a non existing time-series.
func (column TsBlobColumn) GetRanges(rgs ...TsRange) ([]TsBlobPoint, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	var points *C.qdb_ts_blob_point
	var pointsCount C.qdb_size_t
	err := C.qdb_ts_blob_get_ranges(column.parent.handle, alias, columnName, ranges, rangesCount, &points, &pointsCount)

	if err == 0 {
		defer column.parent.Release(unsafe.Pointer(points))
		return blobPointArrayToGo(points, pointsCount), nil
	}
	return nil, ErrorType(err)
}

// GetRanges : Retrieves int64s in the specified range of the time series column.
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
	return nil, ErrorType(err)
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

// Aggregate : Aggregate a sub-part of a timeseries from the specified aggregations.
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

// Aggregate : Aggregate a sub-part of the time series.
//	It is an error to call this function on a non existing time-series.
func (column TsBlobColumn) Aggregate(aggs ...*TsBlobAggregation) ([]TsBlobAggregation, error) {
	alias := convertToCharStar(column.parent.alias)
	defer releaseCharStar(alias)
	columnName := convertToCharStar(column.name)
	defer releaseCharStar(columnName)
	aggregations := blobAggregationArrayToC(aggs...)
	aggregationsCount := C.qdb_size_t(len(aggs))
	var output []TsBlobAggregation
	err := C.qdb_ts_blob_aggregate(column.parent.handle, alias, columnName, aggregations, aggregationsCount)
	if err == 0 {
		output = blobAggregationArrayToGo(aggregations, aggregationsCount, aggs)
	}
	return output, makeErrorOrNil(err)
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
	columns := columnInfoArrayToC(cols...)
	defer releaseColumnInfoArray(columns, len(cols))
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

// Double : adds a double in row transaction
func (t *TsBulk) Double(value float64) *TsBulk {
	if t.err == nil {
		t.err = makeErrorOrNil(C.qdb_ts_row_set_double(t.table, C.qdb_size_t(t.index), C.double(value)))
	}
	t.index++
	return t
}

// Blob : adds a blob in row transaction
func (t *TsBulk) Blob(content []byte) *TsBulk {
	contentSize := C.qdb_size_t(len(content))
	contentPtr := unsafe.Pointer(nil)
	if contentSize != 0 {
		contentPtr = unsafe.Pointer(&content[0])
	}
	if t.err == nil {
		t.err = makeErrorOrNil(C.qdb_ts_row_set_blob(t.table, C.qdb_size_t(t.index), contentPtr, contentSize))
	}
	t.index++
	return t
}

// Int64 : adds an int64 in row transaction
func (t *TsBulk) Int64(value int64) *TsBulk {
	if t.err == nil {
		t.err = makeErrorOrNil(C.qdb_ts_row_set_int64(t.table, C.qdb_size_t(t.index), C.qdb_int_t(value)))
	}
	t.index++
	return t
}

// Timestamp : adds a timestamp in row transaction
func (t *TsBulk) Timestamp(value time.Time) *TsBulk {
	if t.err == nil {
		cValue := toQdbTimespec(value)
		t.err = makeErrorOrNil(C.qdb_ts_row_set_timestamp(t.table, C.qdb_size_t(t.index), &cValue))
	}
	t.index++
	return t
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

// GetDouble : gets a double in row
func (t *TsBulk) GetDouble() (float64, error) {
	var content C.double
	err := C.qdb_ts_row_get_double(t.table, C.qdb_size_t(t.index), &content)
	t.index++
	return float64(content), makeErrorOrNil(err)
}

// GetBlob : gets a blob in row
func (t *TsBulk) GetBlob() ([]byte, error) {
	var content unsafe.Pointer
	defer t.h.Release(content)
	var contentLength C.qdb_size_t
	err := C.qdb_ts_row_get_blob(t.table, C.qdb_size_t(t.index), &content, &contentLength)

	output := C.GoBytes(unsafe.Pointer(content), C.int(contentLength))
	t.index++
	return output, makeErrorOrNil(err)
}

// GetInt64 : gets an int64 in row
func (t *TsBulk) GetInt64() (int64, error) {
	var content C.qdb_int_t
	err := C.qdb_ts_row_get_int64(t.table, C.qdb_size_t(t.index), &content)
	t.index++
	return int64(content), makeErrorOrNil(err)
}

// GetTimestamp : gets a timestamp in row
func (t *TsBulk) GetTimestamp() (time.Time, error) {
	var content C.qdb_timespec_t
	err := C.qdb_ts_row_get_timestamp(t.table, C.qdb_size_t(t.index), &content)
	t.index++
	return content.toStructG(), makeErrorOrNil(err)
}

// GetRanges : create a range bulk query
func (t *TsBulk) GetRanges(rgs ...TsRange) error {
	ranges := rangeArrayToC(rgs...)
	rangesCount := C.qdb_size_t(len(rgs))
	err := C.qdb_ts_table_get_ranges(t.table, ranges, rangesCount)
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
