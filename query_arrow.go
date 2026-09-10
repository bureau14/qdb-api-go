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
// and more than once.
func (r *queryArrowResult) Close() {
	if r == nil || r.result == nil {
		return
	}

	// The C destructor skips columns whose release callback is NULL, so
	// columns moved out by an Arrow import survive this call.
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
func importArrowColumn(col *C.qdb_arrow_column_t) (arrow.Field, arrow.Array, error) { //nolint:ireturn // Justified: arrow.Array is arrow-go's array interface
	name := arrowColumnName(col)
	// cdata's struct types share the C layout of struct ArrowSchema and
	// struct ArrowArray, so the casts are the handoff. The schema is copied
	// and released here.
	field, err := cdata.ImportCArrowField((*cdata.CArrowSchema)(unsafe.Pointer(&col.schema)))
	if err != nil {
		return field, nil, wrapError(C.qdb_e_incompatible_type, "query_arrow_import", "column", name, errorDetailKey, err.Error())
	}

	// The array is moved: the source release callback becomes NULL and the
	// Go array owns the buffers. On error cdata has already released them.
	arr, err := cdata.ImportCArrayWithType((*cdata.CArrowArray)(unsafe.Pointer(&col.data)), field.Type)
	if err != nil {
		return field, nil, wrapError(C.qdb_e_incompatible_type, "query_arrow_import", "column", name, errorDetailKey, err.Error())
	}

	return field, arr, nil
}

// releaseArrowArrays releases every array, skipping nil entries.
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
func arrowRecordFromColumns(fields []arrow.Field, arrays []arrow.Array) (arrow.RecordBatch, error) { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	n := int64(arrays[0].Len())
	// NewRecordBatch panics on a length mismatch; this package returns errors.
	for i, a := range arrays {
		if int64(a.Len()) != n {
			return nil, wrapError(C.qdb_e_invalid_argument, "query_arrow_record", "column", fields[i].Name, "length", a.Len(), "expected", n)
		}
	}

	return array.NewRecordBatch(arrow.NewSchema(fields, nil), arrays, n), nil
}

// importArrowColumns imports every column in order. On success the caller
// owns one reference per array.
func importArrowColumns(cols []C.qdb_arrow_column_t) ([]arrow.Field, []arrow.Array, error) {
	fields := make([]arrow.Field, len(cols))
	arrays := make([]arrow.Array, len(cols))
	for i := range cols {
		field, arr, err := importArrowColumn(&cols[i])
		if err != nil {
			// Unwind what was imported; the rest is still owned by the C
			// result and freed with it.
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
