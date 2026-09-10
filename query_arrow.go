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

// queryArrowResult owns a qdb_query_arrow result until Close.
type queryArrowResult struct {
	// Handle the query was executed on; qdb_release must use the same handle.
	handle HandleType

	// API-allocated result. Nil once closed, or when the statement produced
	// no result set (e.g. DDL).
	result *C.qdb_query_arrow_result_t
}

// executeArrow runs the query through qdb_query_arrow. A non-nil result must
// be closed by the caller, also when it comes with an error.
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

// importArrowColumns imports every column in order. On failure the columns
// imported so far are released and the error returned; the columns not yet
// imported keep their release callbacks and are freed by the C destructor.
// On success the caller owns one reference per array.
func importArrowColumns(cols []C.qdb_arrow_column_t) ([]arrow.Field, []arrow.Array, error) {
	fields := make([]arrow.Field, len(cols))
	arrays := make([]arrow.Array, len(cols))
	for i := range cols {
		field, arr, err := importArrowColumn(&cols[i])
		if err != nil {
			releaseArrowArrays(arrays[:i])

			return nil, nil, err
		}
		fields[i], arrays[i] = field, arr
	}

	return fields, arrays, nil
}

// FetchArrow executes the query and returns its result as an Arrow record
// batch, zero-copy over the buffers the C API allocated.
//
// Args:
//
//	None
//
// Returns:
//
//	arrow.RecordBatch: The result, or nil when the statement produces none (e.g. DDL)
//	error: Query error, if any
//
// The caller owns the batch and must call Release on it. The batch holds no
// reference to the handle and may outlive it. A batch may be returned
// together with an error when the query partially failed; release it either
// way.
//
// Timestamp fields carry no time zone: the C API shifts the values into the
// handle's zone and exports them naive. Attaching the zone is to be done once
// the handle time zone is available on the Go side.
//
// Example:
//
//	rec, err := h.Query("select $timestamp, price from trades in range(today)").FetchArrow()
//	if err != nil {
//	    return err
//	}
//	defer rec.Release()
//	price := rec.Column(rec.Schema().FieldIndices("price")[0]).(*array.Float64)
func (q Query) FetchArrow() (arrow.RecordBatch, error) { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	// Every column is moved out of the C result before the deferred Close
	// releases it; the C destructor skips moved columns, which is what
	// detaches the batch from the handle.
	r, execErr := q.executeArrow()
	if r == nil {
		return nil, execErr
	}
	defer r.Close()

	cols := r.columns()
	if len(cols) == 0 {
		return nil, execErr
	}

	fields, arrays, err := importArrowColumns(cols)
	if err != nil {
		return nil, err
	}
	// The batch retains its own references; these are the importer's.
	defer releaseArrowArrays(arrays)

	rec, err := arrowRecordFromColumns(fields, arrays)
	if err != nil {
		return nil, err
	}

	// execErr is non-nil only on partial failure, where the batch still
	// holds the rows that succeeded.
	return rec, execErr
}
