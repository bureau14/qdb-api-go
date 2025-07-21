// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: QuasarDB Go client API
// Types: Reader, Writer, ColumnData, HandleType
// Ex: h.NewReader(opts).FetchAll() → batch
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"time"
	"unsafe"
)

// WriterTable holds table data for batch push.
type WriterTable struct {
	TableName string

	// All arrays are guaranteed to be of lenght `rowCount`. This means specifically
	// the `idx` parameter and all Writerdata value arrays within `data`.
	rowCount int

	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []WriterColumn

	// An index that enables looking up of a column's offset within the table by its name.
	columnOffsetByName map[string]int

	// The index, can not contain null values
	idx []C.qdb_timespec_t

	// Value arrays to write for each column.
	data []ColumnData
}

// NewWriterTable creates a table with the given columns.
func NewWriterTable(t string, cols []WriterColumn) (WriterTable, error) {
	data := make([]ColumnData, len(cols))

	// Initialize all columns with empty data to prevent nil entries
	for i, col := range cols {
		switch col.ColumnType {
		case TsColumnInt64:
			emptyData := NewColumnDataInt64([]int64{})
			data[i] = &emptyData
		case TsColumnDouble:
			emptyData := NewColumnDataDouble([]float64{})
			data[i] = &emptyData
		case TsColumnString:
			emptyData := NewColumnDataString([]string{})
			data[i] = &emptyData
		case TsColumnBlob:
			emptyData := NewColumnDataBlob([][]byte{})
			data[i] = &emptyData
		case TsColumnTimestamp:
			emptyData := NewColumnDataTimestamp([]time.Time{})
			data[i] = &emptyData
		}
	}

	columnInfoByOffset := make([]WriterColumn, len(cols))
	columnOffsetByName := make(map[string]int)

	for i, col := range cols {
		columnInfoByOffset[i] = col
		columnOffsetByName[col.ColumnName] = i
	}

	return WriterTable{t, 0, columnInfoByOffset, columnOffsetByName, nil, data}, nil
}

// GetName returns the table name.
func (t *WriterTable) GetName() string {
	return t.TableName
}

// RowCount returns the number of rows in the table.
func (t *WriterTable) RowCount() int {
	return t.rowCount
}

// SetIndexFromNative sets the table's timestamp index from C timespecs.
func (t *WriterTable) SetIndexFromNative(idx []C.qdb_timespec_t) {
	t.idx = idx
	t.rowCount = len(idx)
}

// SetIndex sets the table's timestamp index.
func (t *WriterTable) SetIndex(idx []time.Time) {
	t.SetIndexFromNative(TimeSliceToQdbTimespec(idx))
}

// GetIndexAsNative returns the timestamp index as C timespecs.
func (t *WriterTable) GetIndexAsNative() []C.qdb_timespec_t {
	return t.idx
}

// GetIndex returns the table's timestamp index.
func (t *WriterTable) GetIndex() []time.Time {
	return QdbTimespecSliceToTime(t.GetIndexAsNative())
}

// toNativeTableData initialises `out` so it can be passed directly to
// qdb_exp_batch_push_with_options.
//
// Decision rationale:
//   - Avoid copies: the timestamp index and every numeric/timespec column
//     are passed zero-copy by pinning the backing Go slices.
//   - Keep memory-ownership crystal clear: every temporary C allocation
//     made here is registered in a slice and folded into ONE `release`
//     closure returned to the caller.
//
// Key assumptions:
//   - t.idx, t.data and t.columnInfoByOffset have already been validated.
//   - `pinner` outlives the C call that will consume `out`.
//   - Each ColumnData implementation obeys CGO pointer-safety when
//     returning from PinToC.
//
// Performance trade-offs:
//   - Exactly one C allocation for the column envelope (O(#columns));
//     all row-level data stays in Go memory.
//
// Usage example:
//
//	rel, err := tbl.toNativeTableData(&pinner, h, &cTbl.data)
//	if err != nil { … }
//	defer rel()   // single call releases every allocation done here.
func (t *WriterTable) toNativeTableData(pinner *runtime.Pinner, h HandleType, out *C.qdb_exp_batch_push_table_data_t) (func(), error) {
	// Set row and column counts directly.
	out.row_count = C.qdb_size_t(t.rowCount)
	out.column_count = C.qdb_size_t(len(t.data))

	// Index ("timestamps") slice: directly reference underlying Go slice memory.
	if t.idx == nil {
		return func() {}, fmt.Errorf("Index is not set")
	}

	if t.rowCount <= 0 {
		return func() {}, fmt.Errorf("Index provided, but number of rows is 0")
	}

	if len(t.data) == 0 {
		return func() {}, fmt.Errorf("Index provided, but no column data provided")
	}

	// Zero-copy: pin the existing Go slice; safe because qdb_timespec_t has no Go pointers.
	pinner.Pin(&t.idx[0])
	out.timestamps = (*C.qdb_timespec_t)(unsafe.Pointer(&t.idx[0]))

	// Allocate native columns array using the QuasarDB allocator so the
	// memory remains valid after this function returns.
	columnCount := len(t.data)
	cols, err := qdbAllocBuffer[C.qdb_exp_batch_push_column_t](h, columnCount)
	if err != nil {
		return func() {}, err
	}
	colSlice := unsafe.Slice(cols, columnCount)

	var releases []func()

	// Convert each ColumnData to its native counterpart.
	for i, column := range t.columnInfoByOffset {
		elem := &colSlice[i]

		// Pin the Go string backing bytes – zero-copy.
		elem.name = pinStringBytes(pinner, &column.ColumnName)
		elem.data_type = C.qdb_ts_column_type_t(column.ColumnType)

		ptr, rel := t.data[i].PinToC(pinner, h) // zero-copy
		*(*unsafe.Pointer)(unsafe.Pointer(&elem.data[0])) = ptr
		releases = append(releases, rel)
	}

	// Store the pointer to the first element.
	out.columns = cols

	releaseAll := func() {
		for _, f := range releases {
			f()
		}
		// cols was C-allocated, must be freed; timestamps are Go-pinned, no release.
		qdbRelease(h, cols)
	}
	return releaseAll, nil
}

// toNative converts a WriterTable into the flat
// qdb_exp_batch_push_table_t required by the batch writer.
//
// Decision rationale:
//   - Encapsulate every table-level conversion and gather the subordinate
//     column release callbacks into a single closure, mirroring the
//     semantics of multiple defers while incurring only one.
//
// Key assumptions:
//   - `pinner` survives until the batch push completes.
//   - `opts` were validated in Writer.Push.
//
// Performance trade-offs:
//   - No data copies; only minimal envelope allocations done in
//     toNativeTableData.
//
// Usage example:
//
//	rel, err := tbl.toNative(&pinner, h, opts, &cTbl)
//	if err != nil { … }
//	defer rel()
func (t *WriterTable) toNative(pinner *runtime.Pinner, h HandleType, opts WriterOptions, out *C.qdb_exp_batch_push_table_t) (func(), error) {
	var err error

	// Zero-copy: use the Go string bytes directly and pin them so the GC keeps
	// the backing array alive for the entire push.
	out.name = pinStringBytes(pinner, &t.TableName)

	// Zero-initialize the rest of the struct. This should already be the case,
	// but just in case, we are very explicit about all the default values we
	// use.
	//
	// Insert truncate -- not supported yet
	out.truncate_ranges = nil
	out.truncate_range_count = 0

	// Deduplication parameters
	out.deduplication_mode = C.qdb_exp_batch_deduplication_mode_t(opts.dedupMode)

	// Upsert mode requires explicit columns so QuasarDB knows which columns
	// are compared for duplicates.
	if opts.dedupMode == WriterDeduplicationModeUpsert && len(opts.dropDuplicateColumns) == 0 {
		return func() {}, fmt.Errorf("upsert deduplication mode requires drop duplicate columns to be set")
	}

	var releases []func()

	if len(opts.dropDuplicateColumns) > 0 {
		count := len(opts.dropDuplicateColumns)
		ptr, err := qdbAllocBuffer[*C.char](h, count)
		if err != nil {
			return func() {}, err
		}

		releases = append(releases, func() {
			qdbRelease(h, ptr)
		})

		dupSlice := unsafe.Slice(ptr, count)
		for i := range opts.dropDuplicateColumns {
			dupSlice[i] = pinStringBytes(pinner, &opts.dropDuplicateColumns[i])
		}

		out.where_duplicate = ptr
		out.where_duplicate_count = C.qdb_size_t(count)
	} else {
		out.where_duplicate = nil
		out.where_duplicate_count = 0
	}

	// Never automatically create tables
	out.creation = C.qdb_exp_batch_creation_mode_t(C.qdb_exp_batch_dont_create)

	releaseTableData, err := t.toNativeTableData(pinner, h, &out.data)
	if err != nil {
		return releaseTableData, err
	}

	release := func() {
		releaseTableData()

		for _, f := range releases {
			f()
		}
	}

	return release, nil
}

// SetData sets column data at the given offset.
func (t *WriterTable) SetData(offset int, xs ColumnData) error {
	if len(t.columnInfoByOffset) <= offset {
		return fmt.Errorf("Column offset out of range: %v", offset)
	}

	col := t.columnInfoByOffset[offset]
	if col.ColumnType.AsValueType() != xs.ValueType() {
		return fmt.Errorf("Column's expected value type does not match provided value type: column type (%v)'s value type %v != %v", col.ColumnType, col.ColumnType.AsValueType(), xs.ValueType())
	}

	t.data[offset] = xs

	return nil
}

// SetDatas sets data for all columns.
func (t *WriterTable) SetDatas(xs []ColumnData) error {
	for i, x := range xs {
		err := t.SetData(i, x)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetData retrieves column data at the given offset.
func (t *WriterTable) GetData(offset int) (ColumnData, error) {
	if offset >= len(t.data) {
		return nil, fmt.Errorf("Column offset out of range: %v", offset)
	}

	return t.data[offset], nil
}

// MergeWriterTables merges multiple WriterTables by grouping them by table name.
// Tables with the same name must have identical schemas (columns and types).
// This is the primary merge function that handles all cases, including when all
// tables happen to be for the same table name.
// Returns a slice with one WriterTable per unique table name.
func MergeWriterTables(tables []WriterTable) ([]WriterTable, error) {
	if len(tables) <= 1 {
		return tables, nil
	}

	// Performance optimization: check if all tables have unique names first
	uniqueNames := make(map[string]bool)
	for _, table := range tables {
		if uniqueNames[table.TableName] {
			break
		}
		uniqueNames[table.TableName] = true
	}

	// Common case: all tables have unique names
	if len(uniqueNames) == len(tables) {
		return tables, nil
	}

	// Group tables by name
	groups := make(map[string][]WriterTable)
	for _, table := range tables {
		groups[table.TableName] = append(groups[table.TableName], table)
	}

	result := make([]WriterTable, 0, len(groups))
	for tableName, group := range groups {
		if len(group) == 1 {
			result = append(result, group[0])
		} else {
			merged, err := MergeSingleTableWriters(group)
			if err != nil {
				return nil, fmt.Errorf("failed to merge tables for %q: %w", tableName, err)
			}
			result = append(result, merged)
		}
	}

	return result, nil
}

// MergeSingleTableWriters merges multiple WriterTables with the same table name.
// All input tables must have identical table names and column schemas.
// Performance-optimized with pre-allocation based on total row count.
// This is a specialized function for when you know all tables are for the same table.
func MergeSingleTableWriters(tables []WriterTable) (WriterTable, error) {
	if len(tables) == 0 {
		return WriterTable{}, fmt.Errorf("cannot merge empty table slice")
	}

	if len(tables) == 1 {
		return tables[0], nil
	}

	base := tables[0]
	baseName := base.TableName

	// Validate all tables have same name and schema
	totalRows := base.rowCount
	for i := 1; i < len(tables); i++ {
		table := tables[i]
		if table.TableName != baseName {
			return WriterTable{}, fmt.Errorf("table name mismatch: expected %q, got %q", baseName, table.TableName)
		}
		if !writerTableSchemasEqual(base, table) {
			return WriterTable{}, fmt.Errorf("schema mismatch for table %q: tables have different column schemas", baseName)
		}
		// Validate all tables have the same column count
		if len(table.data) != len(base.data) {
			return WriterTable{}, fmt.Errorf("column count mismatch for table %q: expected %d columns, got %d", baseName, len(base.data), len(table.data))
		}
		totalRows += table.rowCount
	}

	// Pre-allocate merged index with total capacity
	mergedIdx := make([]C.qdb_timespec_t, 0, totalRows)

	// Create merged data columns - create new instances based on the value type
	mergedData := make([]ColumnData, len(base.data))
	for i, baseColumn := range base.data {
		// SAFETY: Skip nil columns (can occur when parser skips fields)
		if baseColumn == nil {
			continue
		}
		// Create a new column data of the same type
		switch baseColumn.ValueType() {
		case TsValueInt64:
			newCol := NewColumnDataInt64(nil)
			newCol.EnsureCapacity(totalRows)
			mergedData[i] = &newCol
		case TsValueDouble:
			newCol := NewColumnDataDouble(nil)
			newCol.EnsureCapacity(totalRows)
			mergedData[i] = &newCol
		case TsValueString:
			newCol := NewColumnDataString(nil)
			newCol.EnsureCapacity(totalRows)
			mergedData[i] = &newCol
		case TsValueBlob:
			newCol := NewColumnDataBlob(nil)
			newCol.EnsureCapacity(totalRows)
			mergedData[i] = &newCol
		case TsValueTimestamp:
			newCol := NewColumnDataTimestamp(nil)
			newCol.EnsureCapacity(totalRows)
			mergedData[i] = &newCol
		default:
			return WriterTable{}, fmt.Errorf("unsupported column type: %v", baseColumn.ValueType())
		}
	}

	// Merge all data
	for _, table := range tables {
		// Append timestamp indices
		mergedIdx = append(mergedIdx, table.idx...)

		// Append column data
		for i, column := range table.data {
			// SAFETY: Skip nil columns (can occur when parser skips fields)
			if column == nil || mergedData[i] == nil {
				continue
			}
			err := mergedData[i].appendData(column)
			if err != nil {
				return WriterTable{}, fmt.Errorf("failed to append data for column %d: %w", i, err)
			}
		}
	}

	// Create merged table
	merged := WriterTable{
		TableName:          baseName,
		rowCount:           totalRows,
		columnInfoByOffset: base.columnInfoByOffset,
		columnOffsetByName: base.columnOffsetByName,
		idx:                mergedIdx,
		data:               mergedData,
	}

	return merged, nil
}
