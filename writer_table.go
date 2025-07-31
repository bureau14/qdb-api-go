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

// toNativeTableData prepares table data for C API consumption.
//
// CRITICAL SAFETY PATTERN: This function creates PinnableBuilders to defer
// pointer assignment until AFTER memory is pinned. This prevents segfaults
// from violating Go 1.23+ CGO pointer rules.
//
// The Problem (CAUSES SEGFAULT):
//
//	out.timestamps = (*C.qdb_timespec_t)(unsafe.Pointer(&t.idx[0]))  // CRASH!
//	// ^^ Storing Go pointer in C memory before pinning = immediate panic
//
// The Solution (SAFE):
//
//	pinnableBuilders = append(pinnableBuilders, PinnableBuilder{
//	    Objects: []interface{}{&t.idx[0]},  // What to pin
//	    Builder: func() unsafe.Pointer {  // Executed AFTER pinning
//	        out.timestamps = (*C.qdb_timespec_t)(unsafe.Pointer(&t.idx[0]))
//	        return unsafe.Pointer(&t.idx[0])
//	    },
//	})
//
// Memory strategy by column type:
//   - Timestamps: Zero-copy with pinning (Go slice → pin → C pointer)
//   - Int64/Double: Zero-copy with pinning (Go slice → pin → C pointer)
//   - Blob/String: Copy to C memory (Go slice → copy → C memory, no pinning)
//
// This is Phase 1 of the 5-phase pattern. No pinning or pointer assignment
// happens here - only preparation of builders for later execution.
//
// Returns:
//   - pinnableBuilders: Closures that assign pointers AFTER pinning
//   - release: Cleanup function for all C allocations
//   - error: Any validation or allocation failures
func (t *WriterTable) toNativeTableData(h HandleType, out *C.qdb_exp_batch_push_table_data_t) ([]PinnableBuilder, func(), error) {
	// Set row and column counts directly.
	out.row_count = C.qdb_size_t(t.rowCount)
	out.column_count = C.qdb_size_t(len(t.data))

	// Index ("timestamps") slice: directly reference underlying Go slice memory.
	if t.idx == nil {
		return nil, func() {}, wrapError(C.qdb_e_invalid_argument, "writer_table_to_native", "reason", "index not set")
	}

	if t.rowCount <= 0 {
		return nil, func() {}, wrapError(C.qdb_e_invalid_argument, "writer_table_to_native", "rows", t.rowCount, "reason", "no rows")
	}

	if len(t.data) == 0 {
		return nil, func() {}, wrapError(C.qdb_e_invalid_argument, "writer_table_to_native", "columns", len(t.data), "reason", "no columns")
	}

	// Collect PinnableBuilders for centralized pinning
	var pinnableBuilders []PinnableBuilder

	// Zero-copy: Pass timestamp index directly
	// SAFETY: This demonstrates the critical pattern for all zero-copy operations.
	// We MUST NOT assign the pointer now because t.idx[0] isn't pinned yet!
	// Instead, we create a closure that will execute this assignment later.
	//
	// WRONG (causes segfault):
	//   out.timestamps = (*C.qdb_timespec_t)(unsafe.Pointer(&t.idx[0]))
	//
	// RIGHT (what we do here):
	pinnableBuilders = append(pinnableBuilders, NewPinnableBuilderSingle(&t.idx[0], func() unsafe.Pointer {
		// This code runs in Phase 2.5, AFTER t.idx[0] is pinned
		out.timestamps = (*C.qdb_timespec_t)(unsafe.Pointer(&t.idx[0]))
		return unsafe.Pointer(&t.idx[0])
	}))

	// Allocate native columns array using the QuasarDB allocator so the
	// memory remains valid after this function returns.
	columnCount := len(t.data)
	cols := qdbAllocBuffer[C.qdb_exp_batch_push_column_t](h, columnCount)
	colSlice := unsafe.Slice(cols, columnCount)

	var releases []func()

	// Convert each ColumnData to its native counterpart.
	for i, column := range t.columnInfoByOffset {
		elem := &colSlice[i]

		// Allocate C string for column name
		cName := qdbCopyString(h, column.ColumnName)
		releases = append(releases, func() { qdbReleasePointer(h, unsafe.Pointer(cName)) })
		elem.name = cName
		elem.data_type = C.qdb_ts_column_type_t(column.ColumnType)

		// Convert column data based on type:
		// - Numeric types: Zero-copy with pinning (efficient but requires pinning)
		// - Blob/String: Copy to C memory (2x memory but avoids pointer violations)
		builder, rel := t.data[i].PinToC(h)

		// CRITICAL VARIABLE CAPTURE PATTERN:
		// We MUST capture loop variables to avoid all builders referencing the last element!
		//
		// WRONG (all builders would use the last elem):
		//   Builder: func() { elem.data = ... }  // elem changes each iteration!
		//
		// RIGHT (each builder uses its own elem):
		currentElem := elem     // Capture current iteration's pointer
		localBuilder := builder // Capture current iteration's builder

		wrappedBuilder := NewPinnableBuilderMultiple(localBuilder.Objects, func() unsafe.Pointer {
			// This executes in Phase 2.5 with captured variables
			ptr := localBuilder.Builder()
			// Store the pointer in the C structure
			*(*unsafe.Pointer)(unsafe.Pointer(&currentElem.data[0])) = ptr
			return ptr
		})
		pinnableBuilders = append(pinnableBuilders, wrappedBuilder)
		releases = append(releases, rel)
	}

	// Store the pointer to the first element.
	out.columns = cols

	// Create a single release function that cleans up all allocations
	releaseAll := func() {
		// Release all column-specific allocations (names, blob/string data)
		for _, f := range releases {
			f()
		}
		// Release the column envelope (C-allocated metadata)
		qdbRelease(h, cols)
		// Note: Timestamp index and numeric columns use Go memory - no release needed
	}
	return pinnableBuilders, releaseAll, nil
}

// toNative converts a WriterTable to the C structure for batch push.
//
// This is a wrapper around toNativeTableData that adds table-level
// metadata like name, deduplication settings, and creation mode.
//
// IMPORTANT: This function is part of Phase 1 (Prepare) of the
// centralized pinning strategy. It collects PinnableBuilders but
// does NOT pin them - that happens later in Writer.Push.
//
// The function handles:
//  1. Table name allocation (C memory)
//  2. Deduplication column setup (if enabled)
//  3. Delegation to toNativeTableData for actual data
//  4. Aggregation of all release functions
//
// Memory safety note: All C allocations are tracked in the releases
// slice to ensure proper cleanup even on error paths.
func (t *WriterTable) toNative(h HandleType, opts WriterOptions, out *C.qdb_exp_batch_push_table_t) ([]PinnableBuilder, func(), error) {
	var err error

	var releases []func()

	// Allocate C string for table name
	cTableName := qdbCopyString(h, t.TableName)
	// Add table name release to releases
	releases = append(releases, func() {
		qdbReleasePointer(h, unsafe.Pointer(cTableName))
	})

	out.name = cTableName

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
		return nil, func() {}, wrapError(C.qdb_e_invalid_argument, "writer_table_to_native", "dedup_mode", "upsert", "reason", "missing drop duplicate columns")
	}

	if len(opts.dropDuplicateColumns) > 0 {
		count := len(opts.dropDuplicateColumns)
		ptr := qdbAllocBuffer[*C.char](h, count)

		releases = append(releases, func() {
			qdbRelease(h, ptr)
		})

		dupSlice := unsafe.Slice(ptr, count)
		for i := range opts.dropDuplicateColumns {
			cDupCol := qdbCopyString(h, opts.dropDuplicateColumns[i])
			releases = append(releases, func() { qdbReleasePointer(h, unsafe.Pointer(cDupCol)) })
			dupSlice[i] = cDupCol
		}

		out.where_duplicate = ptr
		out.where_duplicate_count = C.qdb_size_t(count)
	} else {
		out.where_duplicate = nil
		out.where_duplicate_count = 0
	}

	// Never automatically create tables
	out.creation = C.qdb_exp_batch_creation_mode_t(C.qdb_exp_batch_dont_create)

	pinnableObjects, releaseTableData, err := t.toNativeTableData(h, &out.data)
	if err != nil {
		return pinnableObjects, releaseTableData, err
	}

	release := func() {
		releaseTableData()

		for _, f := range releases {
			f()
		}
	}

	return pinnableObjects, release, nil
}

// SetData sets column data at the given offset.
func (t *WriterTable) SetData(offset int, xs ColumnData) error {
	if len(t.columnInfoByOffset) <= offset {
		return wrapError(C.qdb_e_out_of_bounds, "writer_table_set_data", "offset", offset, "max", len(t.columnInfoByOffset)-1)
	}

	col := t.columnInfoByOffset[offset]
	if col.ColumnType.AsValueType() != xs.ValueType() {
		return wrapError(C.qdb_e_incompatible_type, "writer_table_set_data", "column_type", col.ColumnType, "expected_value_type", col.ColumnType.AsValueType(), "provided_value_type", xs.ValueType())
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
		return nil, wrapError(C.qdb_e_out_of_bounds, "writer_table_get_data", "offset", offset, "max", len(t.data)-1)
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
				return nil, wrapError(C.qdb_e_invalid_argument, "merge_writer_tables", "table", tableName, "error", err)
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
		return WriterTable{}, wrapError(C.qdb_e_invalid_argument, "merge_single_table_writers", "reason", "empty table slice")
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
			return WriterTable{}, wrapError(C.qdb_e_invalid_argument, "merge_single_table_writers", "expected", baseName, "actual", table.TableName)
		}
		if !writerTableSchemasEqual(base, table) {
			return WriterTable{}, wrapError(C.qdb_e_invalid_argument, "merge_single_table_writers", "table", baseName, "reason", "schema mismatch")
		}
		// Validate all tables have the same column count
		if len(table.data) != len(base.data) {
			return WriterTable{}, wrapError(C.qdb_e_invalid_argument, "merge_single_table_writers", "table", baseName, "expected_columns", len(base.data), "actual_columns", len(table.data))
		}
		totalRows += table.rowCount
	}

	// Pre-allocate merged index with total capacity
	mergedIdx := make([]C.qdb_timespec_t, 0, totalRows)

	// Create merged data columns - create new instances based on the value type
	mergedData := make([]ColumnData, len(base.data))
	for i, baseColumn := range base.data {
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
			return WriterTable{}, wrapError(C.qdb_e_incompatible_type, "merge_single_table_writers", "column_type", baseColumn.ValueType())
		}
	}

	// Merge all data
	for _, table := range tables {
		// Append timestamp indices
		mergedIdx = append(mergedIdx, table.idx...)

		// Append column data
		for i, column := range table.data {
			err := mergedData[i].appendData(column)
			if err != nil {
				return WriterTable{}, wrapError(C.qdb_e_invalid_argument, "merge_single_table_writers", "column", i, "error", err)
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
