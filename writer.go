// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: QuasarDB Go client API
// Types: Reader, Writer, ColumnData, HandleType
// Ex: h.NewReader(opts).FetchAll() → batch
//
// CRITICAL MEMORY SAFETY PATTERN:
// The Writer implements a 5-phase centralized pinning strategy that is the ONLY
// safe way to pass Go memory to C in this codebase. This pattern prevents
// segfaults from violating Go 1.23+ CGO pointer rules.
//
// The 5-phase pattern in Push() (MANDATORY sequence):
//  1. Prepare: Collect PinnableBuilders (no pointer assignments!)
//  2. Pin: Pin all Go memory at once
//     2.5. Build: Execute builders to populate C structures
//  3. Execute: Call C API with pinned memory
//  4. KeepAlive: Prevent GC collection until done
//
// WHY THIS MATTERS:
// - Direct pointer assignment before pinning = immediate segfault
// - The builder pattern defers assignments until after pinning
// - This is the ONLY pattern that works with GODEBUG=cgocheck=2
//
// Memory strategies by column type:
//   - Int64/Double/Timestamp: Zero-copy with pinning (maximum performance)
//   - Blob/String: Copy to C memory (required for pointer safety)
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/ts.h>
*/
import "C"

import (
	"runtime"
	"time"
	"unsafe"
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
		return wrapError(C.qdb_e_invalid_argument, "writer_set_table", "table", tableName, "reason", "already exists")
	}

	// Ensure schema consistency with previously added tables by comparing to
	// the first table. If a=b and a=c, then b=c.
	for _, existing := range w.tables {
		if !writerTableSchemasEqual(existing, t) {
			return wrapError(C.qdb_e_invalid_argument, "writer_set_table", "table", t.GetName(), "existing_table", existing.GetName(), "reason", "schema mismatch")
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
		return WriterTable{}, wrapError(C.qdb_e_alias_not_found, "writer_get_table", "table", name)
	}

	return t, nil
}

// Length returns the number of tables in the writer.
func (w *Writer) Length() int {
	return len(w.tables)
}

// Push writes all tables to the QuasarDB server.
//
// CRITICAL: This method implements the 5-phase centralized pinning strategy
// that prevents segfaults when passing Go memory to C. This exact sequence
// is MANDATORY - any deviation will cause crashes in production.
//
// Phase 1: Prepare - Convert all data and collect PinnableBuilders
// Phase 2: Pin - Pin all Go memory at once before any C calls
// Phase 2.5: Build - Execute builder closures to populate C structures
// Phase 3: Execute - Call C API with pinned memory
// Phase 4: KeepAlive - Prevent GC collection until C is done
//
// WHY THIS PATTERN: Go 1.23+ forbids storing Go pointers in C memory before
// pinning. The builder pattern defers pointer assignment until after pinning,
// preventing "cgo argument has Go pointer to unpinned Go pointer" panics.
//
// This pattern is essential because:
//   - Go 1.23+ forbids storing Go pointers in C memory before pinning
//   - The C API may access gigabytes of data over 10-10000ms
//   - Premature GC collection would cause segfaults
//   - All memory must remain valid until C completely finishes
func (w *Writer) Push(h HandleType) error {
	// Phase 1 setup: Create pinner and defer cleanup
	var pinner runtime.Pinner
	defer func() {
		pinner.Unpin() // Unpin all objects when done
	}()
	var releases []func() // collected column/table release callbacks

	defer func() {
		// Clean up all C allocations after push completes
		for _, f := range releases {
			f()
		}
	}()

	if w.Length() == 0 {
		return wrapError(C.qdb_e_invalid_argument, "writer_push", "reason", "no tables")
	}

	tblSlice := make([]C.qdb_exp_batch_push_table_t, w.Length())
	i := 0

	// Phase 1: Prepare - Collect all PinnableBuilders from all tables
	// CRITICAL: No pointer assignments happen here! We only prepare builders that will
	// assign pointers AFTER pinning. This is the key to avoiding segfaults.
	//
	// What happens in this phase:
	// - Create C structures with space for pointers (but pointers remain nil)
	// - Create PinnableBuilder closures that capture variables
	// - Collect cleanup functions for C-allocated memory
	//
	// FORBIDDEN in this phase:
	// - tblSlice[i].data = (*C.double)(unsafe.Pointer(&goSlice[0]))  // CRASH!
	// - Any direct pointer assignment to C structures
	var allPinnableBuilders []PinnableBuilder

	for _, v := range w.tables {
		pinnableBuilders, releaseTableData, err := v.toNative(h, w.options, &tblSlice[i])
		if err != nil {
			return wrapError(C.qdb_e_invalid_argument, "writer_push", "table", v.GetName(), "error", err)
		}

		allPinnableBuilders = append(allPinnableBuilders, pinnableBuilders...)
		releases = append(releases, releaseTableData)
		i++
	}

	// Phase 2: Pin - Pin all objects at once
	// CRITICAL: This must happen AFTER all data preparation and BEFORE any C calls.
	// Pinning prevents the GC from moving these objects during the C call.
	//
	// IMPORTANT: Extract actual data pointers from interface{} containers.
	// The Objects array contains interface{} values that wrap actual pointers
	// (like unsafe.Pointer from unsafe.SliceData() or unsafe.StringData()).
	// We must pin the actual data, not the interface{} container.
	//
	for _, builder := range allPinnableBuilders {
		for _, obj := range builder.Objects {
			if obj != nil {
				// Extract the actual pointer from the interface{} container. See
				// ADR-003 for rationale on why this is the best way to approach
				// this.
				switch v := obj.(type) {
				case unsafe.Pointer:
					// Pin the actual data pointer
					pinner.Pin(v)
				case *byte:
					// Pin the actual data pointer (from unsafe.SliceData/StringData)
					pinner.Pin(v)
				case *string:
					// Handle string pointer case
					pinner.Pin(v)
				default:
					// Fallback: pin the interface{} container (legacy behavior)
					pinner.Pin(obj)
				}
			}
		}
	}

	// Phase 2.5: Build pointers after pinning
	// CRITICAL: This phase executes the deferred pointer assignments.
	// Now that objects are pinned and cannot move, it's finally safe to
	// store Go pointers in C-accessible memory.
	//
	// Each builder closure:
	// - Was created in Phase 1 with captured variables
	// - Contains code like: table.data = (*C.double)(unsafe.Pointer(&goSlice[0]))
	// - Is now safe to execute because goSlice[0] is pinned
	//
	// This is the moment where C structures finally get their pointers!
	for _, builder := range allPinnableBuilders {
		if builder.Builder != nil {
			_ = builder.Builder() // Execute builder to populate C structures
		}
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

	// Phase 3: Execute - Call C API with pinned memory
	// WARNING: This call can take 10-10000ms and may access gigabytes of data.
	// All pinned memory MUST remain valid for the entire duration.
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

	// Phase 4: KeepAlive - Prevent GC collection until C is completely done
	// CRITICAL: Even though the C function has returned, it may have spawned
	// background threads or the runtime may still be processing the data.
	// We MUST keep all objects alive to prevent segfaults.
	//
	// This seemingly redundant code is essential for safety. The Go compiler
	// is aggressive about marking objects as dead once they're no longer
	// referenced in Go code. Without these KeepAlive calls, the GC could
	// collect our data while C is still using it, causing crashes.
	//
	// Each KeepAlive prevents a specific failure mode:
	//
	// IMPORTANT: Extract actual data pointers from interface{} containers
	// for KeepAlive, matching the pinning logic above.
	for _, builder := range allPinnableBuilders {
		for _, obj := range builder.Objects {
			if obj != nil {
				// Extract the actual pointer from the interface{} container
				switch v := obj.(type) {
				case unsafe.Pointer:
					// Keep alive the actual data pointer
					runtime.KeepAlive(v)
				case *byte:
					// Keep alive the actual data pointer (from unsafe.SliceData/StringData)
					runtime.KeepAlive(v)
				case *string:
					// Handle string pointer case
					runtime.KeepAlive(v)
				default:
					// Fallback: keep alive the interface{} container (legacy behavior)
					runtime.KeepAlive(obj) // Prevent GC of individual data slices
				}
			}
		}
	}
	runtime.KeepAlive(w)                   // Prevent GC of the writer itself
	runtime.KeepAlive(w.tables)            // Prevent GC of the tables map
	runtime.KeepAlive(allPinnableBuilders) // Prevent GC of the builders slice

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
