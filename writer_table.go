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

// Single table to be provided to the batch writer.
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

// NewWriterTable constructs an empty table definition using the provided columns.
//
// Decision rationale:
//   - Precomputes both column name→offset and offset→column mappings for
//     efficient validation during SetData and Push.
//
// Key assumptions:
//   - cols contains at least one column and no duplicate names.
//
// Performance trade-offs:
//   - Linear initialization to build the lookup maps; negligible for typical
//     column counts.
func NewWriterTable(t string, cols []WriterColumn) (WriterTable, error) {
	data := make([]ColumnData, len(cols))

	columnInfoByOffset := make([]WriterColumn, len(cols))
	columnOffsetByName := make(map[string]int)

	for i, col := range cols {
		columnInfoByOffset[i] = col
		columnOffsetByName[col.ColumnName] = i
	}

	return WriterTable{t, 0, columnInfoByOffset, columnOffsetByName, nil, data}, nil
}

// GetName returns the table identifier used when pushing data.
func (t *WriterTable) GetName() string {
	return t.TableName
}

// RowCount reports the number of rows currently assigned to the table.
func (t *WriterTable) RowCount() int {
	return t.rowCount
}

// SetIndexFromNative sets the timestamp index using a C-compatible slice.
//
// Decision rationale:
//   - Avoids repeated conversions when the caller already holds native timespec
//     values (e.g., from another API call).
//
// Key assumptions:
//   - idx represents the exact row count for subsequent column data.
func (t *WriterTable) SetIndexFromNative(idx []C.qdb_timespec_t) {
	t.idx = idx
	t.rowCount = len(idx)
}

// SetIndex converts times to qdb_timespec_t and stores them as the index.
// This helper is convenient for typical Go callers.
func (t *WriterTable) SetIndex(idx []time.Time) {
	t.SetIndexFromNative(TimeSliceToQdbTimespec(idx))
}

// GetIndexAsNative exposes the internal index slice in C form.
// The caller must treat the slice as read-only.
func (t *WriterTable) GetIndexAsNative() []C.qdb_timespec_t {
	return t.idx
}

// GetIndex returns the index converted back to time.Time values.
func (t *WriterTable) GetIndex() []time.Time {
	return QdbTimespecSliceToTime(t.GetIndexAsNative())
}

func (t *WriterTable) toNativeTableData(pinner *runtime.Pinner, h HandleType, out *C.qdb_exp_batch_push_table_data_t) error {
	// Set row and column counts directly.
	out.row_count = C.qdb_size_t(t.rowCount)
	out.column_count = C.qdb_size_t(len(t.data))

	// Index ("timestamps") slice: directly reference underlying Go slice memory.
	if t.idx == nil {
		return fmt.Errorf("Index is not set")
	}

	if t.rowCount <= 0 {
		return fmt.Errorf("Index provided, but number of rows is 0")
	}

	if len(t.data) == 0 {
		return fmt.Errorf("Index provided, but no column data provided")
	}

	timestampPtr, err := qdbAllocAndCopyBuffer[C.qdb_timespec_t, C.qdb_timespec_t](h, t.idx)
	if err != nil {
		return fmt.Errorf("Unable to copy timestamps: %v", err)
	}
	out.timestamps = timestampPtr

	// Allocate native columns array using the QuasarDB allocator so the
	// memory remains valid after this function returns.
	columnCount := len(t.data)
	cols, err := qdbAllocBuffer[C.qdb_exp_batch_push_column_t](h, columnCount)
	if err != nil {
		return err
	}
	colSlice := unsafe.Slice(cols, columnCount)

	// Convert each ColumnData to its native counterpart.
	for i, column := range t.columnInfoByOffset {

		elem := &colSlice[i]

		// Pin the Go string backing bytes – zero-copy.
		elem.name = pinStringBytes(pinner, &column.ColumnName)
		elem.data_type = C.qdb_ts_column_type_t(column.ColumnType)

		ptr := t.data[i].CopyToC(h)
		*(*unsafe.Pointer)(unsafe.Pointer(&elem.data[0])) = ptr
	}

	// Store the pointer to the first element.
	out.columns = cols

	return nil
}

// toNative converts WriterTable to native C type and avoids copies where possible.
// It is the caller's responsibility to ensure that the WriterTable lives at least
// as long as the native C structure.
func (t *WriterTable) toNative(pinner *runtime.Pinner, h HandleType, opts WriterOptions, out *C.qdb_exp_batch_push_table_t) error {
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
		return fmt.Errorf("upsert deduplication mode requires drop duplicate columns to be set")
	}

	if len(opts.dropDuplicateColumns) > 0 {
		count := len(opts.dropDuplicateColumns)
		ptr, err := qdbAllocBuffer[*C.char](h, count)
		if err != nil {
			return err
		}
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

	err = t.toNativeTableData(pinner, h, &out.data)
	if err != nil {
		return err
	}

	return nil
}

func (t *WriterTable) releaseNative(h HandleType, tbl *C.qdb_exp_batch_push_table_t) error {
	if tbl == nil {
		return fmt.Errorf("WriterTable.releaseNative: nil table pointer")
	}

	columnCount := len(t.data)
	if columnCount == 0 || tbl.data.columns == nil || tbl.name == nil {
		return fmt.Errorf("WriterTable.releaseNative: inconsistent state")
	}

	if tbl.name != nil {
		// Name points to pinned Go memory – just nil it out.
		tbl.name = nil
	}

	if t.idx != nil {
		qdbRelease(h, tbl.data.timestamps)
		tbl.data.timestamps = nil
	}

	if tbl.data.columns != nil {
		// Release any column names we allocated during toNativeTableData
		columnSlice := unsafe.Slice(tbl.data.columns, columnCount)
		err := releaseBatchPushColumns(h, columnSlice, int(tbl.data.row_count))
		if err != nil {
			return err
		}

		qdbRelease(h, tbl.data.columns)
		tbl.data.columns = nil
	}

	if tbl.where_duplicate != nil {
		qdbRelease(h, tbl.where_duplicate)
		tbl.where_duplicate = nil
	}

	return nil
}

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

func (t *WriterTable) SetDatas(xs []ColumnData) error {
	for i, x := range xs {
		err := t.SetData(i, x)

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *WriterTable) GetData(offset int) (ColumnData, error) {
	if offset >= len(t.data) {
		return nil, fmt.Errorf("Column offset out of range: %v", offset)
	}

	return t.data[offset], nil
}
