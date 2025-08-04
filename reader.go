// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: QuasarDB Go client API
// Types: Reader, Writer, ColumnData, HandleType
// Ex: h.NewReader(opts).FetchAll() → batch
package qdb

/*
   #include <stdlib.h>
   #include <string.h> // for memcpy
   #include <qdb/client.h>
   #include <qdb/ts.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"time"
	"unsafe"
)

// ReaderColumn holds column metadata for reading.
type ReaderColumn struct {
	columnName string       // column identifier
	columnType TsColumnType // data type
}

// NewReaderColumn creates column metadata.
func NewReaderColumn(n string, t TsColumnType) (ReaderColumn, error) {
	if !t.IsValid() {
		return ReaderColumn{}, fmt.Errorf("NewReaderColumn: invalid column: %v", t)
	}
	return ReaderColumn{columnName: n, columnType: t}, nil
}

// NewReaderColumnFromNative creates column from C types.
func NewReaderColumnFromNative(n *C.char, t C.qdb_ts_column_type_t) (ReaderColumn, error) {
	if n == nil {
		return ReaderColumn{}, fmt.Errorf("NewReaderColumnFromNative: got null string reference for column name: %v", n)
	}

	return ReaderColumn{
		columnName: C.GoString(n),
		columnType: TsColumnType(t),
	}, nil
}

// Name returns the column name.
func (rc ReaderColumn) Name() string {
	return rc.columnName
}

// Type returns the column data type.
func (rc ReaderColumn) Type() TsColumnType {
	return rc.columnType
}

// ReaderChunk holds a batch of rows read from table.
type ReaderChunk struct {
	// An index that enables looking up of a column's name by its offset within the table.
	columnInfoByOffset []ReaderColumn

	// The index, can not contain null values
	idx []time.Time

	// Value arrays read from each column
	data []ColumnData
}

// NewReaderChunk creates a chunk from columns, index, and data.
func NewReaderChunk(cols []ReaderColumn, idx []time.Time, data []ColumnData) (ReaderChunk, error) {
	return ReaderChunk{
		columnInfoByOffset: cols,
		idx:                idx,
		data:               data,
	}, nil
}

// Empty reports if the chunk has no data.
func (rc *ReaderChunk) Empty() bool {
	// Returns true if no data
	return len(rc.idx) == 0 || len(rc.data) == 0
}

// Clear resets the chunk to empty state.
func (rc *ReaderChunk) Clear() {
	// Empty slice
	rc.idx = make([]time.Time, 0)

	for i := range len(rc.data) {
		rc.data[i].Clear()
	}
}

// EnsureCapacity pre-allocates space for n rows.
func (rc *ReaderChunk) EnsureCapacity(n int) {
	rc.idx = sliceEnsureCapacity(rc.idx, n)

	for i := range len(rc.data) {
		// ReaderData has its own virtual method to ensure capacity,
		// as they're all of different types
		rc.data[i].EnsureCapacity(n)
	}
}

// RowCount returns the number of rows in the chunk.
func (rc *ReaderChunk) RowCount() int {
	return len(rc.idx)
}

// mergeReaderChunks combines multiple chunks into one.
func mergeReaderChunks(xs []ReaderChunk) (ReaderChunk, error) {
	if len(xs) == 0 {
		return ReaderChunk{}, nil
	}

	var base ReaderChunk = xs[0]
	var totalRows int = 0

	// Short-circuit in case there is just a single chunk, which is actuallyu a common case
	if len(xs) == 1 {
		return base, nil
	}

	for i, chunk := range xs[1:] {
		if len(chunk.data) != len(base.data) {
			return base, fmt.Errorf("column length mismatch at chunk %d: expected %d, got %d", i+1, len(base.data), len(chunk.data))
		}
		for ci, c := range chunk.data {
			if c.ValueType() != base.data[ci].ValueType() {
				// you can also pull the name from base.columnInfoByOffset[ci].Name()
				return base, fmt.Errorf(
					"column mismatch at chunk %d, offset %d: expected type %v, got %v",
					i+1, ci,
					base.data[ci].ValueType(), c.ValueType(),
				)
			}
		}
		totalRows += len(chunk.idx)
	}

	mergedIdx := make([]time.Time, 0, totalRows)
	mergedData := make([]ColumnData, len(base.data))

	// Pre-allocate all data, useful when merging many smaller chunks into a larger chunk
	for idx, col := range base.data {
		// Rather than a lot of boilerplate, we just reuse the input object of the
		// base object, and reset that object's content to 0.
		//
		// This keeps the code small.
		//
		// We do need to make sure that we actually get "rid" of the references of
		// the old column, as slices are typically passed by reference, so all cols
		// would be pointing to the same slice reference
		var newCol ColumnData = col

		// Resets the actual held data, but not the column data / name
		newCol.Clear()

		// Ensure that the slice backing array can hold the final merged size
		newCol.EnsureCapacity(totalRows)

		mergedData[idx] = newCol
	}

	for _, chunk := range xs {
		mergedIdx = append(mergedIdx, chunk.idx...)
		for idx, col := range chunk.data {
			err := mergedData[idx].appendData(col)
			if err != nil {
				return ReaderChunk{}, fmt.Errorf("error appending data column %d: %w", idx, err)
			}
		}
	}

	return ReaderChunk{
		idx:                mergedIdx,
		data:               mergedData,
		columnInfoByOffset: base.columnInfoByOffset,
	}, nil
}

// newReaderChunk converts C table data to Go ReaderChunk.
func newReaderChunk(columns []ReaderColumn, data C.qdb_exp_batch_push_table_data_t) (ReaderChunk, error) {
	if data.timestamps == nil {
		return ReaderChunk{}, fmt.Errorf("internal error: nil timestamps")
	}

	if data.columns == nil {
		return ReaderChunk{}, fmt.Errorf("internal error: nil columns")
	}

	if data.row_count <= 0 {
		return ReaderChunk{}, fmt.Errorf("internal error: invalid row count %d", data.row_count)
	}

	if data.column_count <= 0 {
		return ReaderChunk{}, fmt.Errorf("internal error: invalid column count %d", data.column_count)
	}

	var out ReaderChunk
	out.columnInfoByOffset = columns

	var rowCount int = int(data.row_count)

	// Copy index using utility to convert slice of C.qdb_timespec_t to []time.Time
	out.idx = QdbTimespecSliceToTime(unsafe.Slice(data.timestamps, rowCount))

	// Store the column data
	colCount := int(data.column_count)
	out.data = make([]ColumnData, colCount)

	columnSlice := unsafe.Slice(data.columns, colCount)
	for i := 0; i < colCount; i++ {
		column := columnSlice[i]

		expected := C.qdb_ts_column_type_t(columns[i].columnType)
		if column.data_type != expected {
			return ReaderChunk{}, fmt.Errorf("internal error: column %d type mismatch (expected %v, got %v)", i, expected, column.data_type)
		}

		name := columns[i].columnName

		switch columns[i].columnType.AsValueType() {
		case TsValueInt64:
			v, err := newColumnDataInt64FromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueDouble:
			v, err := newColumnDataDoubleFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueTimestamp:
			v, err := newColumnDataTimestampFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueBlob:
			v, err := newColumnDataBlobFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		case TsValueString:
			v, err := newColumnDataStringFromNative(name, column, rowCount)
			if err != nil {
				return ReaderChunk{}, err
			}
			out.data[i] = &v
		default:
			return ReaderChunk{}, fmt.Errorf("internal error: unsupported value type for column %s", name)
		}
	}

	return out, nil
}

// ReaderOptions configures bulk read operations.
type ReaderOptions struct {
	batchSize  int       // max rows per fetch
	tables     []string  // tables to read
	columns    []string  // columns to read
	rangeStart time.Time // range start (inclusive)
	rangeEnd   time.Time // range end (exclusive)
}

// NewReaderOptions creates reader options with defaults.
func NewReaderOptions() ReaderOptions {
	// Default to 32768 rows
	var defaultBatchSize int = 32 * 1024

	return ReaderOptions{batchSize: defaultBatchSize}
}

// NewReaderDefaultOptions creates options for reading entire tables.
func NewReaderDefaultOptions(tables []string) ReaderOptions {
	return NewReaderOptions().WithTables(tables)
}

// WithBatchSize sets max rows per fetch.
func (ro ReaderOptions) WithBatchSize(batchSize int) ReaderOptions {
	ro.batchSize = batchSize
	return ro
}

// WithTables sets tables to read.
func (ro ReaderOptions) WithTables(tables []string) ReaderOptions {
	ro.tables = tables
	return ro
}

// WithColumns sets columns to read (empty=all).
func (ro ReaderOptions) WithColumns(columns []string) ReaderOptions {
	ro.columns = columns
	return ro
}

// WithTimeRange sets time range [start, end).
func (ro ReaderOptions) WithTimeRange(start, end time.Time) ReaderOptions {
	ro.rangeStart = start
	ro.rangeEnd = end

	return ro
}

// Reader iterates over bulk data from QuasarDB.
type Reader struct {
	// Handle that was used to create the reader, and should be reused accross all additional
	// calls (specifically all memory allocations and/or qdb_release() invocations) in the scope
	// of this reader.
	handle HandleType

	// Options that were used to initialize the reader, for future reference if required
	options ReaderOptions

	// Current state of the reader handle.
	state C.qdb_reader_handle_t

	// Iterator pattern: keep track whether we previously ran into an error so the user can
	// look it up
	err error

	// Iterator pattern: true if we reached the end
	done bool

	// Iterator pattern: current batch we're pointing at
	currentBatch ReaderChunk
}

// NewReader creates a reader for bulk data retrieval.
func NewReader(h HandleType, options ReaderOptions) (Reader, error) {
	var ret Reader
	ret.handle = h
	ret.options = options

	// Step 1: validations
	if len(options.tables) == 0 {
		return ret, fmt.Errorf("no tables provided")
	}

	// Either both rangeStart and rangeEnd must be zero (meaning no range
	// filtering) or both must be non-zero.  Having only one of them set is
	// invalid.
	if options.rangeStart.IsZero() != options.rangeEnd.IsZero() {
		return ret, fmt.Errorf("invalid time range")
	}

	if !options.rangeEnd.IsZero() && !options.rangeEnd.After(options.rangeStart) {
		return ret, fmt.Errorf("invalid time range")
	}

	// Step 1: validate that our batchSize makes sense -- that it's not exceptionally large
	if options.batchSize <= 0 || options.batchSize > (1<<24) {
		return ret, fmt.Errorf("invalid batch size: %d", options.batchSize)
	}

	// Only ever a single range, so we can stack-allocate it and share directly with
	// the C API invocation.
	var cRanges [1]C.qdb_ts_range_t
	var cRangeCount C.qdb_size_t = 0

	// Important: we pass a null-pointer to the C API in case no ranges are provided.
	//
	// By relying on the functionality of qdb_bulk reader's C implementation that null
	// ranges implies "forever", we avoid the issue of having to find a way to
	// "represent" a forever range
	var cRangePtr *C.qdb_ts_range_t = nil

	// But if a range is provided, set the pointer accordingly.
	if !options.rangeStart.IsZero() && !options.rangeEnd.IsZero() {
		cRanges[0] = toQdbRange(options.rangeStart, options.rangeEnd)

		// Points to stack-allocated value
		cRangePtr = &cRanges[0]
		cRangeCount = 1
	}

	// Step 3: Initialize `C.qdb_bulk_reader_table_t` structs. Even though our C API allows for using
	// different ranges per table, we use a fixed range for all tables. As such, we can reuse the
	// previously constructed time ranges for all tables.  The table array itself is allocated via
	// the QuasarDB allocator so it is compatible with the C API.  Each table name must also be
	// allocated using qdbCopyString and is immediately deferred for release.
	tableCount := len(options.tables)
	cTables := qdbAllocBuffer[C.qdb_bulk_reader_table_t](h, tableCount)
	defer qdbRelease(h, cTables)
	tblSlice := unsafe.Slice(cTables, tableCount)

	for i, tbl := range options.tables {
		name := qdbCopyString(h, tbl)
		defer qdbRelease(h, name)

		tblSlice[i].name = name
		tblSlice[i].ranges = cRangePtr
		tblSlice[i].range_count = cRangeCount
	}

	// Step 4: Initialize `columns` arguments.  Column names are optional.  If provided we allocate
	// an array of char* pointers and copy each string using qdbCopyString.  The allocated memory is
	// freed immediately after calling into the C API.
	columnCount := len(options.columns)
	var cColumns **C.char
	if columnCount > 0 {
		ptr := qdbAllocBuffer[*C.char](h, columnCount)
		defer qdbRelease(h, ptr)
		colSlice := unsafe.Slice(ptr, columnCount)
		for i, col := range options.columns {
			cname := qdbCopyString(h, col)
			defer qdbRelease(h, cname)
			colSlice[i] = cname
		}
		cColumns = ptr
	} else {
		cColumns = nil
	}

	// Step 5: invoke qdb_bulk_reader_fetch() which initializes the native reader handle.  After
	// this call the C API manages its own copy of the provided tables and columns so we can
	// safely release our temporary allocations via the deferred qdbRelease calls.
	var readerHandle C.qdb_reader_handle_t
	errCode := C.qdb_bulk_reader_fetch(
		ret.handle.handle,
		cColumns,
		C.qdb_size_t(columnCount),
		cTables,
		C.qdb_size_t(tableCount),
		&readerHandle,
	)

	err := wrapError(errCode, "reader_init", "tables", tableCount)
	if err != nil {
		return ret, err
	}

	ret.state = readerHandle

	// Done, return state.

	return ret, nil
}

// fetchBatch retrieves the next batch of rows.
func (r *Reader) fetchBatch() (ReaderChunk, error) {
	var ret ReaderChunk
	var ptr *C.qdb_bulk_reader_table_data_t

	// Time the C API call
	start := time.Now()

	// qdb_bulk_reader_get_data "fills" a pointer in style of when you would get data back
	// for a number of tables, but it returns just a pointer for a single table. all memory
	// allocated within this function call is linked to this single object, and a qdbRelease
	// clears eerything
	errCode := C.qdb_bulk_reader_get_data(r.state, &ptr, C.qdb_size_t(r.options.batchSize))
	err := wrapError(errCode, "reader_fetch_batch", "batch_size", r.options.batchSize)

	elapsed := time.Since(start)

	// Trigger the `defer` statement as there are failure scenarios in both cases where err
	// is nil or not-nil. That's why we put the ptr-check before checking error return codes.
	if ptr != nil {
		// All data in the `ptr`is allocated on the QuasarDB C-API side,
		// and as such is linked to this one "root" pointer.
		//
		// By invoking qdbRelease on it, it automatically recursively releases
		// all attached objects.
		defer qdbRelease(r.handle, ptr)
	}

	if errors.Is(err, ErrIteratorEnd) {
		if ptr != nil {
			return ret, fmt.Errorf("fetchBatch iterator end, did not expect table data: %v", ptr)
		}

		// Return empty batch
		return ret, nil
	} else if err != nil {
		return ret, fmt.Errorf("unable to fetch bulk reader data: %v", err)
	} else if ptr == nil {
		return ret, fmt.Errorf("qdb_bulk_reader_get_data returned nil pointer")
	}

	table := *ptr

	if table.columns == nil || table.column_count <= 0 {
		return ret, fmt.Errorf("invalid column metadata (columns=%v column_count=%d)", table.columns, table.column_count)
	}

	colCount := int(table.column_count)
	colSlice := unsafe.Slice(table.columns, colCount)

	cols := make([]ReaderColumn, colCount)
	for j := range colCount {
		cols[j], err = NewReaderColumnFromNative(colSlice[j].name, colSlice[j].data_type)
		if err != nil {
			return ret, err
		}
	}

	ret, err = newReaderChunk(cols, table)
	if err != nil {
		return ret, err
	}

	// Log the batch read performance
	rowCount := ret.RowCount()
	L().Debug("read batch of rows", "count", rowCount, "duration", elapsed)

	return ret, nil
}

// Next advances to the next batch, returns false when done.
func (r *Reader) Next() bool {
	if r.done {
		return false
	}

	var err error
	r.currentBatch, err = r.fetchBatch()

	if errors.Is(err, ErrIteratorEnd) {
		r.done = true
		return false
	}

	if err != nil || r.currentBatch.Empty() {
		r.err = err // Only store non-ErrIteratorEnd errors
		r.done = true
		return false
	}

	return true
}

// Err returns any error from iteration.
func (r *Reader) Err() error {
	return r.err
}

// Batch returns the current batch.
func (r *Reader) Batch() ReaderChunk {
	return r.currentBatch
}

// FetchAll retrieves all data as a single batch.
func (r *Reader) FetchAll() (ReaderChunk, error) {
	// Accumulate all batches from the reader's iterator, and merges them together in a single
	// batch.

	var ret ReaderChunk
	var err error

	// Allocate batches with an initial capacity to avoid frequent reallocations
	// because we expect potentially many batches for large reads.
	batches := make([]ReaderChunk, 0, 4)

	// Iterate until we've collected all available data batches
	for r.Next() {
		batch := r.Batch()
		batches = append(batches, batch)
	}

	if r.Err() != nil {
		return ret, fmt.Errorf("Reader.FetchAll: error while traversing data: %v", r.Err())
	}

	ret, err = mergeReaderChunks(batches)
	if err != nil {
		return ret, err
	}

	return ret, nil
}

// Close releases reader resources.
func (r *Reader) Close() {
	// if state is non-nil, invoke qdbRelease() on state
	if r.state != nil {

		qdbReleasePointer(r.handle, unsafe.Pointer(r.state))

		r.state = nil
	}
}
