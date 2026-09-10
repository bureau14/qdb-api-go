// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/query.h>

	#cgo noescape qdb_query_arrow
	#cgo nocallback qdb_query_arrow
*/
import "C"

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// queryArrowResult holds the Arrow columns returned by qdb_query_arrow. The
// wrapper is API-allocated and tracked by the handle; it lives until Close is
// called or the handle is closed. Each column is an Arrow C Data Interface
// pair (ArrowSchema, ArrowArray) whose buffers belong to the Arrow exporter
// inside libqdb_api, not to the qdb allocator.
type queryArrowResult struct {
	// Handle the query was executed on; qdb_release must use the same handle.
	handle HandleType

	// API-allocated result. Nil once closed, or when the statement produced
	// no result set (e.g. DDL).
	result *C.qdb_query_arrow_result_t
}

// executeArrow runs the query through qdb_query_arrow. A nil result with a
// nil error means the statement produced no result set. A non-nil result may
// come back together with an error (partial failure); the caller owns it
// either way and must Close it.
func (q Query) executeArrow() (*queryArrowResult, error) {
	query := convertToCharStar(q.query)
	defer releaseCharStar(query)
	r := queryArrowResult{handle: q.HandleType}
	err := C.qdb_query_arrow(q.handle, query, &r.result)
	if r.result == nil {
		return nil, wrapError(err, "query_execute_arrow", "query", q.query)
	}

	return &r, wrapError(err, "query_execute_arrow", "query", q.query)
}

// Close releases the API-allocated result. Safe to call on a nil receiver
// and more than once. The C destructor calls each column's release callback
// only when it is still set, so columns whose buffers were moved out by an
// Arrow import are skipped and their buffers stay alive.
func (r *queryArrowResult) Close() {
	if r == nil || r.result == nil {
		return
	}

	qdbReleasePointer(r.handle, unsafe.Pointer(r.result))
	// Barrier-free nil store; see setCPtr.
	setCPtr(unsafe.Pointer(&r.result), nil)
}

// columns views the C column array as a slice, without copying. The view is
// valid until Close; a nil or closed receiver yields nil.
func (r *queryArrowResult) columns() []C.qdb_arrow_column_t {
	if r == nil || r.result == nil || r.result.columns == nil {
		return nil
	}

	return unsafe.Slice(r.result.columns, int(r.result.column_count))
}

// arrowColumnName reads the column name from the exported schema. The C
// side writes the query's column name, or an empty string when the
// expression has none.
func arrowColumnName(col *C.qdb_arrow_column_t) string {
	if col.schema.name == nil {
		return ""
	}

	return C.GoString(col.schema.name)
}

// arrowColumnMoved reports whether both C Data Interface structs of the
// column have been released or moved out: the Arrow convention marks a
// consumed struct by setting its release callback to NULL.
func arrowColumnMoved(col *C.qdb_arrow_column_t) bool {
	return col.schema.release == nil && col.data.release == nil
}

// importArrowColumn moves one C column into a Go arrow.Field and arrow.Array.
// The caller owns the returned array and must Release it.
//
// The two cgo struct types are distinct Go types over the same C layout
// (struct ArrowSchema, struct ArrowArray), so the casts are the whole
// handoff. The schema is copied and released by cdata. The array is moved:
// cdata memcpy's the struct into its own allocation and sets the source
// release callback to NULL, after which the Go array owns the buffers and
// calls the producer's release from its own Release path. The C destructor
// skips a column whose release is NULL, so qdb_release on the wrapper no
// longer touches these buffers and the array outlives the handle. On an
// import error cdata has already released whatever it moved, so the caller
// has nothing to undo for this column.
func importArrowColumn(col *C.qdb_arrow_column_t) (arrow.Field, arrow.Array, error) { //nolint:ireturn // Justified: arrow.Array is arrow-go's array interface
	name := arrowColumnName(col)
	field, err := cdata.ImportCArrowField((*cdata.CArrowSchema)(unsafe.Pointer(&col.schema)))
	if err != nil {
		return field, nil, wrapError(C.qdb_e_incompatible_type, "query_arrow_import", "column", name, errorDetailKey, err.Error())
	}

	arr, err := cdata.ImportCArrayWithType((*cdata.CArrowArray)(unsafe.Pointer(&col.data)), field.Type)
	if err != nil {
		return field, nil, wrapError(C.qdb_e_incompatible_type, "query_arrow_import", "column", name, errorDetailKey, err.Error())
	}

	return field, arr, nil
}

// releaseArrowArrays releases every non-nil array. Used to drop the
// importer's references once a record batch holds its own, and to unwind
// the columns imported before a failure.
func releaseArrowArrays(xs []arrow.Array) {
	for _, x := range xs {
		if x != nil {
			x.Release()
		}
	}
}

// arrowRecordFromColumns assembles imported columns into one record batch.
// The batch retains its own reference to every array; the caller keeps, and
// later releases, the references it holds.
//
// Every array must have the same length. The C side guarantees this: one
// projection loop fills every column from the same row set, so a mismatch
// would be a C-side bug. It is still checked here because NewRecordBatch
// panics on inconsistent columns and this package returns errors instead.
func arrowRecordFromColumns(fields []arrow.Field, arrays []arrow.Array) (arrow.RecordBatch, error) { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	n := int64(arrays[0].Len())
	for i, a := range arrays {
		if int64(a.Len()) != n {
			return nil, wrapError(C.qdb_e_invalid_argument, "query_arrow_record", "column", fields[i].Name, "length", a.Len(), "expected", n)
		}
	}

	return array.NewRecordBatch(arrow.NewSchema(fields, nil), arrays, n), nil
}
