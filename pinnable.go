// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package qdb: high-performance time series database client
// Types: ErrorType, Handle, Entry, Cluster
// Ex: qdb.Connect(uri).GetBlob(alias) → data
package qdb

import (
	"unsafe"
)

// PinnableBuilder implements the critical CGO memory safety pattern required by Go 1.23+.
//
// # Why This Pattern Exists
//
// Go 1.23 introduced stricter CGO pointer rules that forbid storing Go pointers in
// C-accessible memory before those pointers are pinned. Violating this rule causes
// immediate segfaults when GODEBUG=cgocheck=2 is enabled (which it is in production).
//
// # The Problem We Solve
//
// Consider this WRONG approach that causes segfaults:
//
//	var table C.qdb_table_t
//	table.data = (*C.double)(unsafe.Pointer(&goSlice[0]))  // CRASH: Go pointer in C memory!
//	pinner.Pin(&goSlice[0])                                // Too late - already crashed
//	C.qdb_push_data(&table)
//
// The crash happens because we stored a Go pointer (&goSlice[0]) in C-accessible
// memory (table.data) BEFORE pinning it. The Go runtime detects this and panics.
//
// # The Solution: Deferred Population
//
// PinnableBuilder separates the "what to pin" from "how to use it":
//
//	builder := NewPinnableBuilderSingle(&goSlice[0], func() unsafe.Pointer {
//	    table.data = (*C.double)(unsafe.Pointer(&goSlice[0]))
//	    return unsafe.Pointer(&goSlice[0])
//	})
//	// Later, in the correct sequence:
//	for _, obj := range builder.Objects {
//	    pinner.Pin(obj)      // 1. Pin first
//	}
//	builder.Builder()        // 2. Then populate C structures
//	C.qdb_push_data(&table)  // 3. Finally call C
//
// # Constructors
//
// Two constructors are provided for convenience:
//   - NewPinnableBuilderSingle: For pinning a single object
//   - NewPinnableBuilderMultiple: For pinning multiple objects (e.g., individual strings)
//
// # Field Usage
//
// Objects: Array of Go memory to pin. Empty for C-allocated memory.
// Builder: Closure that populates C structures. Executed AFTER pinning is complete.
//
// # CRITICAL: This is the ONLY safe pattern for passing Go memory to C in this codebase.
type PinnableBuilder struct {
	Objects []interface{}         // Objects to pin (empty for C-allocated memory)
	Builder func() unsafe.Pointer // Closure executed AFTER pinning
}

// NewPinnableBuilderSingle creates a PinnableBuilder for a single object.
// Use this for numeric types (int64, float64, timestamp) that pin their first element.
//
// Parameters:
//   - object: The Go object to pin (nil for C-allocated memory)
//   - builder: Function that populates C structures after pinning
//
// Example:
//
//	builder := NewPinnableBuilderSingle(&slice[0], func() unsafe.Pointer {
//	    cStruct.data = (*C.double)(unsafe.Pointer(&slice[0]))
//	    return unsafe.Pointer(&slice[0])
//	})
func NewPinnableBuilderSingle(object interface{}, builder func() unsafe.Pointer) PinnableBuilder {
	if object == nil {
		return PinnableBuilder{
			Objects: nil,
			Builder: builder,
		}
	}

	return PinnableBuilder{
		Objects: []interface{}{object},
		Builder: builder,
	}
}

// NewPinnableBuilderMultiple creates a PinnableBuilder for multiple objects.
// Use this for string/blob types that need to pin individual elements.
//
// Parameters:
//   - objects: Array of Go objects to pin
//   - builder: Function that populates C structures after pinning
//
// Example:
//
//	objects := make([]interface{}, 0, len(strings))
//	for i := range strings {
//	    if len(strings[i]) > 0 {
//	        objects = append(objects, &strings[i])
//	    }
//	}
//	builder := NewPinnableBuilderMultiple(objects, func() unsafe.Pointer {
//	    // Populate C structures with pinned string data
//	    return unsafe.Pointer(envelope)
//	})
func NewPinnableBuilderMultiple(objects []interface{}, builder func() unsafe.Pointer) PinnableBuilder {
	return PinnableBuilder{
		Objects: objects,
		Builder: builder,
	}
}
