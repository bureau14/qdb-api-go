package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// Metadata we need to represent a single column.
type WriterColumn struct {
	ColumnName string
	ColumnType TsColumnType
}

type Writer struct {
	options WriterOptions
	tables  map[string]WriterTable
}

// releaseBatchPushBlobColumns releases the memory of a slice of qdb_blob_t.
// Each blob may own individually allocated content buffers.
func releaseBatchPushBlobColumns(h HandleType, xs []C.qdb_blob_t) {
	for _, x := range xs {
		if x.content != nil {
			C.qdb_release(h.handle, x.content)
		}
	}
}

// releaseBatchPushStringColumns releases memory owned by qdb_string_t elements.
func releaseBatchPushStringColumns(h HandleType, xs []C.qdb_string_t) {
	for _, x := range xs {
		if x.data != nil {
			qdbRelease(h, x.data)
		}
	}
}

// releaseBatchPushColumn frees all allocations associated with a single push column.
//
// Decision rationale:
//   - Consolidates cleanup logic for WriterTable.releaseNative.
//   - Handles type-specific allocations for strings and blobs.
func releaseBatchPushColumn(h HandleType, x C.qdb_exp_batch_push_column_t, rowCount int) error {
	if x.name != nil {
		qdbRelease(h, x.name)
	}

	// Extract the pointer stored in the union field. We must read the pointer
	// value instead of taking the address of the union field, otherwise we pass
	// a pointer to Go stack memory to C which triggers the cgo "Go pointer to
	// unpinned Go pointer" check.
	dataPtr := *(*unsafe.Pointer)(unsafe.Pointer(&x.data[0]))
	if dataPtr != nil {

		// For blobs and strings, we need to go through the extra effort of releasing
		// their internally allocated data.
		switch x.data_type {
		case C.qdb_ts_column_blob:
			xs := unsafe.Slice((*C.qdb_blob_t)(dataPtr), rowCount)
			releaseBatchPushBlobColumns(h, xs)
		case C.qdb_ts_column_string:
			xs := unsafe.Slice((*C.qdb_string_t)(dataPtr), rowCount)
			releaseBatchPushStringColumns(h, xs)
		}

		qdbReleasePointer(h, dataPtr)
	}

	return nil
}

// releaseBatchPushColumns iterates releaseBatchPushColumn over the provided slice.
func releaseBatchPushColumns(h HandleType, xs []C.qdb_exp_batch_push_column_t, rowCount int) error {
	for _, x := range xs {
		err := releaseBatchPushColumn(h, x, rowCount)
		if err != nil {
			return err
		}
	}
	return nil
}

// Creates a new Writer with the provided options
func NewWriter(options WriterOptions) Writer {
	return Writer{options: options, tables: make(map[string]WriterTable)}
}

// Creates a new Writer with default options
func NewWriterWithDefaultOptions() Writer {
	return NewWriter(NewWriterOptions())
}

// Returns the writer's options
func (w *Writer) GetOptions() WriterOptions {
	return w.options
}

// Sets the data of a table. Returns error if table already exists.
func (w *Writer) SetTable(t WriterTable) error {
	tableName := t.GetName()

	// Check if the table already exists
	_, exists := w.tables[tableName]
	if exists {
		return fmt.Errorf("table %q already exists", tableName)
	}

	// Ensure schema consistency with previously added tables by comparing to
	// the first table. If a=b and a=c, then b=c.
	for _, existing := range w.tables {
		if !writerTableSchemasEqual(existing, t) {
			return fmt.Errorf("table %q schema differs from existing table %q", t.GetName(), existing.GetName())
		}
		break
	}

	w.tables[tableName] = t

	return nil
}

// Returns the table with the provided name
func (w *Writer) GetTable(name string) (WriterTable, error) {
	t, ok := w.tables[name]
	if !ok {
		return WriterTable{}, fmt.Errorf("Table not found: %s", name)
	}

	return t, nil
}

// Returns the number of tables the writer currently holds.
func (w *Writer) Length() int {
	return len(w.tables)
}

// Pushes all tables to the server according to PushOptions.
func (w *Writer) Push(h HandleType) error {
	var pinner runtime.Pinner
	defer pinner.Unpin()

	if w.Length() == 0 {
		return fmt.Errorf("No tables to push")
	}

	tblSlice := make([]C.qdb_exp_batch_push_table_t, w.Length())
	i := 0

	for _, v := range w.tables {
		err := v.toNative(&pinner, h, w.options, &tblSlice[i])
		if err != nil {
			// Potential memory leak occurs here, but if we cannot do this conversion,
			// it means something is very wrong and the user should close the handle
			// anyway (and all memory is allocated+tracked using qdbAlloc anyway)
			return fmt.Errorf("Failed to convert table %q to native: %v", v.GetName(), err)
		}
		defer v.releaseNative(h, &tblSlice[i])
		i++
	}

	var tableSchemas = (**C.qdb_exp_batch_push_table_schema_t)(nil)

	var options C.qdb_exp_batch_options_t
	options = w.options.setNative(options)

	errCode := C.qdb_exp_batch_push_with_options(
		h.handle,
		&options,
		&tblSlice[0],
		tableSchemas,
		C.qdb_size_t(len(tblSlice)),
	)
	return makeErrorOrNil(C.qdb_error_t(errCode))
}

// writerTableSchemasEqual returns true when both tables have the same column
// names and types in identical order.
func writerTableSchemasEqual(a, b WriterTable) bool {
	if len(a.columnInfoByOffset) != len(b.columnInfoByOffset) {
		return false
	}
	for i := range a.columnInfoByOffset {
		if a.columnInfoByOffset[i] != b.columnInfoByOffset[i] {
			return false
		}
	}
	return true
}
