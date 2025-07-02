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

// WriterTable: table data for batch push.
// Invariants: len(idx)=rowCount, len(data[i])=rowCount
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

// GetName returns table identifier.
// Returns:
//   string: table name
// Example:
//   name := t.GetName() // → "metrics"
func (t *WriterTable) GetName() string {
	return t.TableName
}

// RowCount returns row count.
// Returns:
//   int: number of rows
// Example:
//   n := t.RowCount() // → 1000
func (t *WriterTable) RowCount() int {
	return t.rowCount
}

// SetIndexFromNative sets native timestamps.
// Args:
//   idx: C timespec array
// Example:
//   t.SetIndexFromNative(cTimes) // sets index
func (t *WriterTable) SetIndexFromNative(idx []C.qdb_timespec_t) {
	t.idx = idx
	t.rowCount = len(idx)
}

// SetIndex sets timestamp index.
// Args:
//   idx: timestamps
// Example:
//   t.SetIndex(times) // sets row timestamps
func (t *WriterTable) SetIndex(idx []time.Time) {
	t.SetIndexFromNative(TimeSliceToQdbTimespec(idx))
}

// GetIndexAsNative returns native timestamps.
// Returns:
//   []C.qdb_timespec_t: C format times
// Example:
//   cTimes := t.GetIndexAsNative() // → []timespec
func (t *WriterTable) GetIndexAsNative() []C.qdb_timespec_t {
	return t.idx
}

// GetIndex returns timestamp index.
// Returns:
//   []time.Time: row timestamps
// Example:
//   times := t.GetIndex() // → []time.Time
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
 //   rel, err := tbl.toNativeTableData(&pinner, h, &cTbl.data)
 //   if err != nil { … }
 //   defer rel()   // single call releases every allocation done here.
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
 //   rel, err := tbl.toNative(&pinner, h, opts, &cTbl)
 //   if err != nil { … }
 //   defer rel()
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

// SetData assigns column data by offset.
// Args:
//   offset: column index
//   xs: data to set
// Returns:
//   error: if offset invalid or type mismatch
// Example:
//   err := t.SetData(0, colData) // → nil or error
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

// SetDatas assigns all column data.
// Args:
//   xs: data for each column
// Returns:
//   error: if any assignment fails
// Example:
//   err := t.SetDatas(allData) // → nil or error
func (t *WriterTable) SetDatas(xs []ColumnData) error {
	for i, x := range xs {
		err := t.SetData(i, x)

		if err != nil {
			return err
		}
	}

	return nil
}

// GetData retrieves column data by offset.
// Args:
//   offset: column index
// Returns:
//   ColumnData: column data
//   error: if offset invalid
// Example:
//   data, err := t.GetData(0) // → ColumnData or error
func (t *WriterTable) GetData(offset int) (ColumnData, error) {
	if offset >= len(t.data) {
		return nil, fmt.Errorf("Column offset out of range: %v", offset)
	}

	return t.data[offset], nil
}
