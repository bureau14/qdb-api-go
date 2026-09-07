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
	"math"
	"time"
	"unsafe"
)

// The payload loads below are plain Go loads over a C cell, so a table
// conversion makes no cgo call after qdb_query returns. They depend on the
// layout of qdb_point_result_t on every supported platform: a 24-byte
// record with the 4-byte tag at offset 0 and the 16-byte union at offset 8,
// which cgo exposes as [16]uint8; qdb_int_t, qdb_size_t and qdb_time_t are
// 8 bytes; qdb_timespec_t is tv_sec then tv_nsec; loads are host-endian,
// the same endianness the C library wrote with; double is IEEE-754
// binary64. The sizes are pinned here at compile time (a mismatch is a
// negative constant, which does not fit uint); the offsets are pinned by
// TestQueryTableCellLayout.
const (
	_ = uint(unsafe.Sizeof(C.qdb_point_result_t{}) - 24)
	_ = uint(24 - unsafe.Sizeof(C.qdb_point_result_t{}))
	_ = uint(unsafe.Sizeof(C.qdb_timespec_t{}) - 16)
	_ = uint(16 - unsafe.Sizeof(C.qdb_timespec_t{}))
	_ = uint(unsafe.Sizeof(C.qdb_size_t(0)) - 8)
	_ = uint(8 - unsafe.Sizeof(C.qdb_size_t(0)))
	_ = uint(unsafe.Sizeof(C.qdb_int_t(0)) - 8)
	_ = uint(8 - unsafe.Sizeof(C.qdb_int_t(0)))
	_ = uint(unsafe.Sizeof(C.qdb_time_t(0)) - 8)
	_ = uint(8 - unsafe.Sizeof(C.qdb_time_t(0)))
)

const (
	nanosPerSecond = int64(time.Second)
	// Bounds on tv_sec inside which tv_sec * nanosPerSecond fits int64.
	maxTimespecSec = math.MaxInt64 / nanosPerSecond
	minTimespecSec = math.MinInt64 / nanosPerSecond
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

// cellPayload is the address of the 16-byte union. Taken from the field,
// not computed from the cell address, so it stays right if cgo ever
// renders the union as a named type.
func cellPayload(c *C.qdb_point_result_t) unsafe.Pointer {
	return unsafe.Pointer(&c.payload)
}

// cellInt64 reads payload.int64_.value, or payload.count.value as the same
// eight bytes.
func cellInt64(c *C.qdb_point_result_t) int64 {
	return *(*int64)(cellPayload(c))
}

// cellDouble reads payload.double_.value.
func cellDouble(c *C.qdb_point_result_t) float64 {
	return *(*float64)(cellPayload(c))
}

// cellTimespec reads payload.timestamp.value: tv_sec at union offset 0,
// tv_nsec at 8.
func cellTimespec(c *C.qdb_point_result_t) (sec, nsec int64) {
	p := cellPayload(c)

	return *(*int64)(p), *(*int64)(unsafe.Add(p, 8))
}

// cellLength reads the qdb_size_t at union offset 8, the content_length of
// a string or blob cell. Only meaningful when the tag is string or blob.
func cellLength(c *C.qdb_point_result_t) uint64 {
	return *(*uint64)(unsafe.Add(cellPayload(c), 8))
}

// cellBytes views the content of a string or blob cell: the pointer sits at
// union offset 0 and its length at 8. Nil for zero length, so unsafe.Slice
// never sees a nil pointer. The view aliases the C result: the caller
// copies it out at once and never stores it.
func cellBytes(c *C.qdb_point_result_t) []byte {
	n := cellLength(c)
	if n == 0 {
		return nil
	}

	return unsafe.Slice((*byte)(*(*unsafe.Pointer)(cellPayload(c))), n)
}

// cellNanos converts a timespec to nanoseconds since the Unix epoch and
// reports false when the result does not fit int64, which is the years
// 1678 to 2262, the same bound the server's Arrow conversion applies.
// tv_nsec is not assumed normalised, so the add is checked as well.
func cellNanos(sec, nsec int64) (int64, bool) {
	// Bounding sec first makes the multiply provably safe: the bound times
	// nanosPerSecond is inside int64 by construction.
	if sec < minTimespecSec || sec > maxTimespecSec {
		return 0, false
	}

	whole := sec * nanosPerSecond
	total := whole + nsec
	// A signed add overflowed exactly when the result's sign differs from
	// the sign of both operands.
	if (whole^total)&(nsec^total) < 0 {
		return 0, false
	}

	return total, true
}

// varSizes is the sizing half of pass one for string and blob columns. It
// sums every cell's length so each column's byte buffer is allocated once,
// and rejects a column whose total does not fit the int32 offsets. Only
// lengths are read, never the content pointer, and only from typed cells:
// a none cell's payload is unspecified.
func varSizes(rows QueryRows, kinds []columnKind, names []string) ([]int, error) {
	sizes := make([]int, len(kinds))
	for _, row := range rows {
		cells := cellsOf(row, len(kinds))
		for j, kind := range kinds {
			if kind != kindString && kind != kindBlob {
				continue
			}
			if cells[j]._type == C.qdb_query_result_none {
				continue
			}

			// sizes[j] never exceeds math.MaxInt32, so the subtraction cannot
			// go negative and a hostile length cannot wrap the comparison.
			n := cellLength(&cells[j])
			if n > math.MaxInt32-uint64(sizes[j]) {
				return nil, wrapError(C.qdb_e_out_of_bounds, "query_var_sizes", "column", names[j], "bytes", n)
			}
			sizes[j] += int(n)
		}
	}

	return sizes, nil
}

// allocColumns builds one concrete column per probed kind with every buffer
// at its final length, so pass two writes cells in place and never grows a
// slice. sizes holds the byte total of each string and blob column and is
// ignored for the other kinds. Null slots are written by appendCell with
// the QDB_IS_NULL_* sentinel of the kind; the bitmap is authoritative.
func allocColumns(names []string, kinds []columnKind, n int, sizes []int) []QueryColumn {
	cols := make([]QueryColumn, len(names))
	for j, kind := range kinds {
		switch kind {
		case kindNone:
			cols[j] = newNullColumn(names[j], n)
		case kindInt64:
			cols[j] = newInt64Column(names[j], n)
		case kindDouble:
			cols[j] = newDoubleColumn(names[j], n)
		case kindTimestamp:
			cols[j] = newTimestampColumn(names[j], n)
		case kindString:
			cols[j] = newStringColumn(names[j], n, sizes[j])
		case kindBlob:
			cols[j] = newBlobColumn(names[j], n, sizes[j])
		}
	}

	return cols
}

// appendRow is pass two for one row: each cell goes to the column pass one
// chose for it. A NullColumn takes no writes, because pass one made the
// column null only when every one of its cells was none.
func appendRow(cols []QueryColumn, row *QueryPoint, i int) error {
	cells := cellsOf(row, len(cols))
	for j := range cells {
		cell := &cells[j]

		var err error
		switch c := cols[j].(type) {
		case *Int64Column:
			c.appendCell(i, cell)
		case *DoubleColumn:
			c.appendCell(i, cell)
		case *TimestampColumn:
			err = c.appendCell(i, cell)
		case *StringColumn:
			c.appendCell(i, cell)
		case *BlobColumn:
			c.appendCell(i, cell)
		case *NullColumn:
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// tableFromRows converts rows into a QueryTable in two passes over the
// row-major C data, both row-first because a row's cells are contiguous.
// It takes no QueryResult so a continuous-query callback can convert a
// batch it does not own, and so unit tests need no hand-built
// qdb_query_result_t. names has one entry per column of every row.
func tableFromRows(names []string, rows QueryRows, scanned int64) (*QueryTable, error) {
	// Pass one settles every column's kind from the type tags alone. From
	// here on a column is either typed or null; mixed and array columns
	// have already been rejected.
	kinds, err := probeKinds(rows, names)
	if err != nil {
		return nil, err
	}

	// Still pass one: string and blob columns need their byte totals so the
	// shared buffer is allocated exactly once per column.
	sizes, err := varSizes(rows, kinds, names)
	if err != nil {
		return nil, err
	}

	// Every buffer is at its final length, so pass two only writes in place.
	cols := allocColumns(names, kinds, len(rows), sizes)

	// Pass two copies the payloads out of C memory. Only a timestamp
	// outside the int64 nanosecond range can fail here.
	for i, row := range rows {
		err = appendRow(cols, row, i)
		if err != nil {
			return nil, err
		}
	}

	return newQueryTable(cols, len(rows), scanned), nil
}

// ToTable copies the result into Go memory as a QueryTable that holds no C
// pointers. A nil or closed receiver yields an empty table and no error.
// The receiver stays open: the caller still closes it, and may do so as
// soon as ToTable returns.
func (r *QueryResult) ToTable() (*QueryTable, error) {
	if r == nil || r.result == nil {
		return newQueryTable(nil, 0, 0), nil
	}

	return tableFromRows(r.ColumnsNames(), r.rowsUnsafe(), r.ScannedPoints())
}

// Fetch executes the query and returns its rows as a QueryTable. The C
// result is released before Fetch returns, so the table needs no Close and
// holds no C pointers. A statement that produces no result set (DDL)
// yields a nil table and a nil error.
//
// Example:
//
//	tbl, err := h.Query("select $timestamp, price from trades in range(today)").Fetch()
//	if err != nil {
//	    return err
//	}
//	price, err := qdb.ColumnOf[*qdb.DoubleColumn](tbl, "price")
func (q Query) Fetch() (*QueryTable, error) {
	r, err := q.Execute()
	if err != nil {
		// Execute may hand back a result together with its error; Close is
		// nil-safe, so releasing unconditionally covers both cases.
		r.Close()

		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	// Deferred so the C result is released even when ToTable fails.
	defer r.Close()

	return r.ToTable()
}
