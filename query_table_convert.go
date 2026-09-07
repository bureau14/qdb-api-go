// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

/*
	#include <qdb/query.h>
*/
import "C"

import (
	"unsafe"
)

// columnKind is the inferred type of a result column. The C API carries no
// column types: a result is a grid of tagged cells, and a column's type is
// whatever its non-null cells agree on. The C API's own Arrow conversion
// and the Python bindings infer it the same way.
type columnKind int8

const (
	kindNone columnKind = iota
	kindInt64
	kindDouble
	kindTimestamp
	kindString
	kindBlob
)

// String names the kind for error context.
func (k columnKind) String() string {
	switch k {
	case kindNone:
		return "none"
	case kindInt64:
		return "int64"
	case kindDouble:
		return "double"
	case kindTimestamp:
		return "timestamp"
	case kindString:
		return "string"
	case kindBlob:
		return "blob"
	default:
		return "unknown"
	}
}

// cellsOf views the contiguous cells of one row. The slice aliases the C
// result and is valid until it is closed; the converter copies everything
// out before it returns.
func cellsOf(row *QueryPoint, n int) []C.qdb_point_result_t {
	return unsafe.Slice((*C.qdb_point_result_t)(unsafe.Pointer(row)), n)
}

// cellKind maps a cell tag to a column kind; column names the column for
// error context.
//
// count folds into int64. Its payload is a qdb_size_t that pass two reads
// as the same eight bytes, so a count of 2^63 lands on the null sentinel
// and larger counts wrap negative. Count results do not occur in practice
// and both precedents (qdb-api-python probe_column_type, the C API Arrow
// path) fold the same way.
//
// Array tags are rejected by value before the switch: they are unsupported
// server-side, and keeping them out of QueryResultValueType keeps the
// public enum and Get untouched until the server supports them.
func cellKind(tag C.qdb_query_result_value_type_t, column string) (columnKind, error) {
	if tag >= C.qdb_query_result_array_double {
		return kindNone, wrapError(C.qdb_e_not_implemented, "query_cell_kind", "column", column, "tag", int64(tag))
	}

	switch QueryResultValueType(tag) {
	case QueryResultNone:
		return kindNone, nil
	case QueryResultInt64, QueryResultCount:
		return kindInt64, nil
	case QueryResultDouble:
		return kindDouble, nil
	case QueryResultTimestamp:
		return kindTimestamp, nil
	case QueryResultString:
		return kindString, nil
	case QueryResultBlob:
		return kindBlob, nil
	default:
		return kindNone, wrapError(C.qdb_e_incompatible_type, "query_cell_kind", "column", column, "tag", int64(tag))
	}
}

// mergeKind folds one cell kind into a column's running kind and reports
// false when the two conflict. none is the identity on both sides: a null
// cell says nothing about the column, and a column that has only seen
// nulls takes the first typed cell it meets.
func mergeKind(have, got columnKind) (columnKind, bool) {
	if have == kindNone {
		return got, true
	}
	if got == kindNone || got == have {
		return have, true
	}

	return kindNone, false
}

// probeKinds is pass one over the result. It reads the type tag of every
// cell and nothing else, settling each column's kind before any payload is
// touched so that pass two can allocate every column at its final size.
//
// The walk is row-first: a row's cells are contiguous 24-byte records, and
// a column-first walk would stride by 24 * len(names) bytes per cell.
func probeKinds(rows QueryRows, names []string) ([]columnKind, error) {
	// Every column starts as none, which is also its final kind when the
	// result has no rows or the column holds only nulls.
	kinds := make([]columnKind, len(names))

	for _, row := range rows {
		cells := cellsOf(row, len(names))
		for j := range cells {
			got, err := cellKind(cells[j]._type, names[j])
			if err != nil {
				return nil, err
			}

			merged, ok := mergeKind(kinds[j], got)
			if !ok {
				return nil, wrapError(C.qdb_e_incompatible_type, "query_probe_kinds",
					"column", names[j], "have", kinds[j], "got", got)
			}
			kinds[j] = merged
		}
	}

	return kinds, nil
}
