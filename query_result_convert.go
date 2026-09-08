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

const (
	nanosPerSecond = int64(time.Second)
	// Bounds on tv_sec inside which tv_sec * nanosPerSecond fits int64.
	maxTimespecSec = math.MaxInt64 / nanosPerSecond
	minTimespecSec = math.MinInt64 / nanosPerSecond
)

// cellsOf views the contiguous cells of one row. The slice aliases the C
// result and is valid until it is closed; the converter copies everything
// out before it returns.
func cellsOf(row *QueryPoint, n int) []C.qdb_point_result_t {
	return unsafe.Slice((*C.qdb_point_result_t)(unsafe.Pointer(row)), n)
}

// cellValueType maps a cell tag to the TsValueType of its column; column
// names the column for error context.
func cellValueType(tag C.qdb_query_result_value_type_t, column string) (TsValueType, error) {
	// Array results are unsupported server-side and have no Go constant.
	// Rejecting them by value here keeps QueryResultValueType and Get as
	// they are until the server supports arrays.
	if tag >= C.qdb_query_result_array_double {
		return TsValueNull, wrapError(C.qdb_e_not_implemented, "query_cell_value_type", "column", column, "tag", int64(tag))
	}

	switch QueryResultValueType(tag) {
	case QueryResultNone:
		return TsValueNull, nil
	case QueryResultInt64, QueryResultCount:
		// A count(...) aggregate cell folds into the int64 column: its
		// qdb_size_t payload is read as the same eight bytes.
		return TsValueInt64, nil
	case QueryResultDouble:
		return TsValueDouble, nil
	case QueryResultTimestamp:
		return TsValueTimestamp, nil
	case QueryResultString:
		return TsValueString, nil
	case QueryResultBlob:
		return TsValueBlob, nil
	default:
		return TsValueNull, wrapError(C.qdb_e_incompatible_type, "query_cell_value_type", "column", column, "tag", int64(tag))
	}
}

// mergeValueType folds one cell's value type into a column's running type
// and reports false when the two conflict. Null is the identity on both
// sides: a null cell says nothing about the column, and a column that has
// only seen nulls takes the first typed cell it meets.
func mergeValueType(have, got TsValueType) (TsValueType, bool) {
	if have == TsValueNull {
		return got, true
	}
	if got == TsValueNull || got == have {
		return have, true
	}

	return TsValueNull, false
}

// probeValueTypes is pass one over the result. It reads the type tag of
// every cell and nothing else, settling each column's value type before
// any payload is touched so that pass two can allocate every column at its
// final size.
//
// The walk is row-first: a row's cells are contiguous 24-byte records, and
// a column-first walk would stride by 24 * len(names) bytes per cell.
func probeValueTypes(rows QueryRows, names []string) ([]TsValueType, error) {
	// Every column starts as null, which is also its final type when the
	// result has no rows or the column holds only nulls.
	types := make([]TsValueType, len(names))

	for _, row := range rows {
		cells := cellsOf(row, len(names))
		for j := range cells {
			got, err := cellValueType(cells[j]._type, names[j])
			if err != nil {
				return nil, err
			}

			merged, ok := mergeValueType(types[j], got)
			if !ok {
				return nil, wrapError(C.qdb_e_incompatible_type, "query_probe_value_types",
					"column", names[j], "have", types[j], "got", got)
			}
			types[j] = merged
		}
	}

	return types, nil
}

// Compile-time layout assertions for the payload loads that follow
// (cellPayload, cellInt64, cellDouble, cellTimespec, cellLength, cellBytes).
// Those functions read a C cell with plain Go loads at fixed offsets rather
// than calling into C, which saves one cgo transition per cell. That is only
// correct while the C types have the sizes the loads assume, so each size is
// pinned twice. unsafe.Sizeof of a fixed-size type is a compile-time
// constant, and converting a negative constant to uint is a compile error
// ("constant -N overflows uint"): if a future C API changes one of these
// sizes, the build fails here instead of the loads silently reading the
// wrong bytes. Field offsets need a variable to name a field, so they are
// pinned by TestQueryResultSetCellLayout instead.
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

// cellPayload is the address of the 16-byte union. Taken from the field,
// not computed from the cell address, so it stays right if cgo ever
// renders the union as a named type.
//
// Layout the loads depend on, on every supported platform: a cell is a
// 24-byte record with the 4-byte tag at offset 0 and the union at offset
// 8, which cgo exposes as [16]uint8; qdb_int_t, qdb_size_t and qdb_time_t
// are 8 bytes; qdb_timespec_t is tv_sec then tv_nsec; loads are host-endian,
// the same endianness the C library wrote with; double is IEEE-754 binary64.
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
// 1678 to 2262. tv_nsec is not assumed normalised, so the add is checked
// as well.
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
// sums every cell's length so each column's shared buffer is allocated
// once, and rejects a column whose total exceeds int32 offsets. Only lengths
// are read, never the content pointer, and only from typed cells: a none
// cell's payload is unspecified.
func varSizes(rows QueryRows, types []TsValueType, names []string) ([]int, error) {
	// One running byte total per column; fixed-width columns stay at zero
	// and are never read by allocColumns.
	sizes := make([]int, len(types))
	for _, row := range rows {
		cells := cellsOf(row, len(types))
		for j, vt := range types {
			// Pass one already settled the column types, so the cells of a
			// fixed-width column are skipped without looking at them.
			if vt != TsValueString && vt != TsValueBlob {
				continue
			}
			// A null cell contributes nothing, and its payload must not be
			// read: the C API leaves it unspecified, so the length field
			// could hold anything.
			if cells[j]._type == C.qdb_query_result_none {
				continue
			}

			// Cell boundaries are int32 offsets into the buffer, so the
			// running total is capped at math.MaxInt32. sizes[j] never
			// exceeds that cap, so the subtraction cannot go negative, and
			// comparing the new length against the remaining room means a
			// hostile length cannot wrap the sum past the cap.
			n := cellLength(&cells[j])
			if n > math.MaxInt32-uint64(sizes[j]) {
				return nil, wrapError(C.qdb_e_out_of_bounds, "query_var_sizes", "column", names[j], "bytes", n)
			}
			sizes[j] += int(n)
		}
	}

	return sizes, nil
}

// allocColumns builds one concrete column per probed value type with every
// buffer at its final length, so pass two writes cells in place and never
// grows a slice. sizes holds the byte total of each string and blob column
// and is ignored for the other types. Null slots are written by appendCell
// with the QDB_IS_NULL_* sentinel of the type; the mask is authoritative.
func allocColumns(names []string, types []TsValueType, n int, sizes []int) []QueryColumn {
	cols := make([]QueryColumn, len(names))
	for j, vt := range types {
		switch vt {
		case TsValueNull:
			cols[j] = newQueryColumnNull(names[j], n)
		case TsValueInt64:
			cols[j] = newQueryColumnInt64(names[j], n)
		case TsValueDouble:
			cols[j] = newQueryColumnDouble(names[j], n)
		case TsValueTimestamp:
			cols[j] = newQueryColumnTimestamp(names[j], n)
		case TsValueString:
			cols[j] = newQueryColumnString(names[j], n, sizes[j])
		case TsValueBlob:
			cols[j] = newQueryColumnBlob(names[j], n, sizes[j])
		}
	}

	return cols
}

// appendRow is pass two for one row: each cell goes to the column pass one
// chose for it. A QueryColumnNull takes no writes, because pass one made the
// column null only when every one of its cells was none.
func appendRow(cols []QueryColumn, row *QueryPoint, i int) error {
	cells := cellsOf(row, len(cols))
	for j := range cells {
		cell := &cells[j]

		var err error
		switch c := cols[j].(type) {
		case *QueryColumnInt64:
			c.appendCell(i, cell)
		case *QueryColumnDouble:
			c.appendCell(i, cell)
		case *QueryColumnTimestamp:
			err = c.appendCell(i, cell)
		case *QueryColumnString:
			c.appendCell(i, cell)
		case *QueryColumnBlob:
			c.appendCell(i, cell)
		case *QueryColumnNull:
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// resultSetFromRows converts rows into a QueryResultSet in two passes over the
// row-major C data, both row-first because a row's cells are contiguous.
// It takes no QueryResult so a continuous-query callback can convert a
// batch it does not own, and so unit tests need no hand-built
// qdb_query_result_t. names has one entry per column of every row.
func resultSetFromRows(names []string, rows QueryRows, scanned int64) (*QueryResultSet, error) {
	// Pass one settles every column's value type from the tags alone. From
	// here on a column is either typed or null; mixed and array columns
	// have already been rejected.
	types, err := probeValueTypes(rows, names)
	if err != nil {
		return nil, err
	}

	// Still pass one: string and blob columns need their byte totals so the
	// shared buffer is allocated exactly once per column.
	sizes, err := varSizes(rows, types, names)
	if err != nil {
		return nil, err
	}

	// Every buffer is at its final length, so pass two only writes in place.
	cols := allocColumns(names, types, len(rows), sizes)

	// Pass two copies the payloads out of C memory. Only a timestamp
	// outside the int64 nanosecond range can fail here.
	for i, row := range rows {
		err = appendRow(cols, row, i)
		if err != nil {
			return nil, err
		}
	}

	return newQueryResultSet(cols, len(rows), scanned), nil
}

// ToResultSet copies the result into Go memory as a QueryResultSet that holds no C
// pointers. A nil or closed receiver yields an empty result set and no error.
// The receiver stays open: the caller still closes it, and may do so as
// soon as ToResultSet returns.
func (r *QueryResult) ToResultSet() (*QueryResultSet, error) {
	if r == nil || r.result == nil {
		return newQueryResultSet(nil, 0, 0), nil
	}

	return resultSetFromRows(r.ColumnsNames(), r.rowsUnsafe(), r.ScannedPoints())
}

// Fetch executes the query and returns its rows as a QueryResultSet. The C
// result is released before Fetch returns, so the result set needs no Close and
// holds no C pointers. A statement that produces no result set (DDL)
// yields a nil result set and a nil error.
//
// Example:
//
//	rs, err := h.Query("select $timestamp, price from trades in range(today)").Fetch()
//	if err != nil {
//	    return err
//	}
//	price, err := qdb.ColumnOf[*qdb.QueryColumnDouble](rs, "price")
func (q Query) Fetch() (*QueryResultSet, error) {
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
	// Deferred so the C result is released even when ToResultSet fails.
	defer r.Close()

	return r.ToResultSet()
}
