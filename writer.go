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
)

// WriterColumn: metadata for single column.
// Fields:
//   ColumnName: identifier
//   ColumnType: data type
type WriterColumn struct {
	ColumnName string
	ColumnType TsColumnType
}

// Writer: batches tables for push.
// Fields:
//   options: push configuration
//   tables: name→WriterTable map
type Writer struct {
	options WriterOptions
	tables  map[string]WriterTable
}

// NewWriter creates writer with options.
// Args:
//   options: push configuration
// Returns:
//   Writer: configured writer
// Example:
//   w := NewWriter(opts) // → Writer{opts, empty map}
func NewWriter(options WriterOptions) Writer {
	return Writer{options: options, tables: make(map[string]WriterTable)}
}

// NewWriterWithDefaultOptions creates writer with defaults.
// Returns:
//   Writer: default configuration
// Example:
//   w := NewWriterWithDefaultOptions() // → Writer{default opts}
func NewWriterWithDefaultOptions() Writer {
	return NewWriter(NewWriterOptions())
}

// GetOptions returns push configuration.
// Returns:
//   WriterOptions: current options
// Example:
//   opts := w.GetOptions() // → WriterOptions
func (w *Writer) GetOptions() WriterOptions {
	return w.options
}

// SetTable adds table to batch.
// Args:
//   t: table to add
// Returns:
//   error: if exists or schema mismatch
// Example:
//   err := w.SetTable(tbl) // → nil or error
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

// GetTable retrieves table by name.
// Args:
//   name: table identifier
// Returns:
//   WriterTable: found table
//   error: if not found
// Example:
//   tbl, err := w.GetTable("my_table") // → WriterTable or error
func (w *Writer) GetTable(name string) (WriterTable, error) {
	t, ok := w.tables[name]
	if !ok {
		return WriterTable{}, fmt.Errorf("Table not found: %s", name)
	}

	return t, nil
}

// Length returns table count.
// Returns:
//   int: number of tables
// Example:
//   n := w.Length() // → 3
func (w *Writer) Length() int {
	return len(w.tables)
}

// Push writes all tables to server.
// Args:
//   h: connection handle
// Returns:
//   error: push failure
// Example:
//   err := w.Push(handle) // → nil or error
func (w *Writer) Push(h HandleType) error {
	var pinner runtime.Pinner
	defer pinner.Unpin()
	var releases []func() // collected column/table release callbacks

	defer func() {
		for _, f := range releases {
			f()
		}
	}()

	if w.Length() == 0 {
		return fmt.Errorf("No tables to push")
	}

	tblSlice := make([]C.qdb_exp_batch_push_table_t, w.Length())
	i := 0

	for _, v := range w.tables {
		releaseTableData, err := v.toNative(&pinner, h, w.options, &tblSlice[i])
		if err != nil {
			return fmt.Errorf("Failed to convert table %q to native: %v", v.GetName(), err)
		}
		releases = append(releases, releaseTableData)
		i++
	}

	var tableSchemas = (**C.qdb_exp_batch_push_table_schema_t)(nil)

	var options C.qdb_exp_batch_options_t
	options = w.options.setNative(options)

	// Count total rows across all tables
	totalRows := 0
	for _, table := range w.tables {
		totalRows += table.RowCount()
	}

	start := time.Now()
	errCode := C.qdb_exp_batch_push_with_options(
		h.handle,
		&options,
		&tblSlice[0],
		tableSchemas,
		C.qdb_size_t(len(tblSlice)),
	)
	elapsed := time.Since(start)
	
	if errCode == 0 {
		L().Info("wrote rows", "count", totalRows, "duration", elapsed)
	}
	
	return makeErrorOrNil(C.qdb_error_t(errCode))
}

// writerTableSchemasEqual compares table schemas.
// In: a, b WriterTable - tables to compare
// Out: bool - true if identical
// Ex: writerTableSchemasEqual(t1, t2) → true
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
