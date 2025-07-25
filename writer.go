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
)

// WriterColumn holds column metadata.
type WriterColumn struct {
	ColumnName string       // column identifier
	ColumnType TsColumnType // data type
}

// Writer batches tables for bulk push.
type Writer struct {
	options WriterOptions          // push configuration
	tables  map[string]WriterTable // table cache
}

// NewWriter creates a writer with options.
func NewWriter(options WriterOptions) Writer {
	return Writer{options: options, tables: make(map[string]WriterTable)}
}

// NewWriterWithDefaultOptions creates a writer with default options.
func NewWriterWithDefaultOptions() Writer {
	return NewWriter(NewWriterOptions())
}

// GetOptions returns the writer's push configuration.
func (w *Writer) GetOptions() WriterOptions {
	return w.options
}

// SetTable adds a table to the writer batch.
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

// GetTable retrieves a table by name from the writer.
func (w *Writer) GetTable(name string) (WriterTable, error) {
	t, ok := w.tables[name]
	if !ok {
		return WriterTable{}, fmt.Errorf("Table not found: %s", name)
	}

	return t, nil
}

// Length returns the number of tables in the writer.
func (w *Writer) Length() int {
	return len(w.tables)
}

// Push writes all tables to the QuasarDB server.
func (w *Writer) Push(h HandleType) error {
	var pinner runtime.Pinner
	defer func() {
		pinner.Unpin()
	}()
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

	tableSchemas := (**C.qdb_exp_batch_push_table_schema_t)(nil)

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

	return wrapError(C.qdb_error_t(errCode), "writer_write", "tables", len(tblSlice))
}

// writerTableSchemasEqual compares schemas of two tables.
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
