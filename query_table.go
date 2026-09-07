// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

/*
	#include <qdb/client.h>
	#include <qdb/query.h>
*/
import "C"

import (
	"math"
	"reflect"
	"time"
	"unsafe"
)

// QueryColumn is one materialised result column of a QueryTable. The
// concrete types are closed: Int64Column, DoubleColumn, TimestampColumn,
// StringColumn, BlobColumn and NullColumn. Consumers dispatch with a type
// switch or ColumnOf.
//
// Every column exposes its buffers directly (Values, Nanos, Offsets, Bytes)
// so vectorised code can run over them without a copy. The column treats
// those buffers as immutable and the caller must too. Accessors never
// consult the validity bitmap: a null slot holds the writer's null sentinel
// and Valid is the contract.
type QueryColumn interface {
	// Name is the column name as reported by the query, duplicates included.
	Name() string
	// Len is the number of rows.
	Len() int
	// Valid is the validity bitmap: bit i set means row i holds a value.
	Valid() Bitmap
	sealed()
}

// Int64Column holds int64 cells as a dense Values slice plus a validity
// bitmap. A count(...) aggregate also lands here, its
// unsigned payload reinterpreted as int64. Null slots hold math.MinInt64,
// the QDB_IS_NULL_INT64 sentinel, so Values can be handed to
// NewColumnDataInt64 and written back as null.
type Int64Column struct {
	// Values is the dense cell buffer, indexed by row. Read-only.
	Values []int64
	name   string
	valid  Bitmap
}

func newInt64Column(name string, n int) *Int64Column {
	return &Int64Column{Values: make([]int64, n), name: name, valid: newBitmap(n)}
}

// Name returns the column name.
func (c *Int64Column) Name() string {
	return c.name
}

// Len returns the number of rows.
func (c *Int64Column) Len() int {
	return len(c.Values)
}

// Valid returns the validity bitmap.
func (c *Int64Column) Valid() Bitmap {
	return c.valid
}

func (c *Int64Column) sealed() {}

// appendCell writes row i from cell. A none cell leaves the bit clear and
// stores the QDB_IS_NULL_INT64 sentinel: the bitmap is authoritative, the
// sentinel only keeps Values usable by the writer.
func (c *Int64Column) appendCell(i int, cell *C.qdb_point_result_t) {
	if cell._type == C.qdb_query_result_none {
		c.Values[i] = math.MinInt64

		return
	}

	// int64 and count share this path: pass one accepted both tags for
	// this column, and the count payload is read as the same eight bytes.
	c.Values[i] = cellInt64(cell)
	c.valid.set(i)
}

// DoubleColumn holds double cells as a dense Values slice plus a validity
// bitmap. Null slots hold NaN, the QDB_IS_NULL_DOUBLE sentinel, so Values
// round-trips through NewColumnDataDouble as null. Values are IEEE-754
// binary64, the C double on every supported platform.
type DoubleColumn struct {
	// Values is the dense cell buffer, indexed by row. Read-only.
	Values []float64
	name   string
	valid  Bitmap
}

func newDoubleColumn(name string, n int) *DoubleColumn {
	return &DoubleColumn{Values: make([]float64, n), name: name, valid: newBitmap(n)}
}

// Name returns the column name.
func (c *DoubleColumn) Name() string {
	return c.name
}

// Len returns the number of rows.
func (c *DoubleColumn) Len() int {
	return len(c.Values)
}

// Valid returns the validity bitmap.
func (c *DoubleColumn) Valid() Bitmap {
	return c.valid
}

func (c *DoubleColumn) sealed() {}

// appendCell writes row i from cell. A none cell leaves the bit clear and
// stores NaN, the QDB_IS_NULL_DOUBLE sentinel.
func (c *DoubleColumn) appendCell(i int, cell *C.qdb_point_result_t) {
	if cell._type == C.qdb_query_result_none {
		c.Values[i] = math.NaN()

		return
	}

	c.Values[i] = cellDouble(cell)
	c.valid.set(i)
}

// TimestampColumn holds timestamp cells as int64 nanoseconds since the Unix
// epoch: one 8-byte value per row with no pointer, so the column compares
// and sorts like any numeric slice. The representable range is the years
// 1678 to 2262; a cell outside it fails conversion with ErrOutOfBounds.
// Null slots hold math.MinInt64.
type TimestampColumn struct {
	// Nanos is the dense cell buffer, indexed by row. Read-only.
	Nanos []int64
	name  string
	valid Bitmap
}

func newTimestampColumn(name string, n int) *TimestampColumn {
	return &TimestampColumn{Nanos: make([]int64, n), name: name, valid: newBitmap(n)}
}

// Name returns the column name.
func (c *TimestampColumn) Name() string {
	return c.name
}

// Len returns the number of rows.
func (c *TimestampColumn) Len() int {
	return len(c.Nanos)
}

// Valid returns the validity bitmap.
func (c *TimestampColumn) Valid() Bitmap {
	return c.valid
}

// Time converts row i to a UTC time.Time. Unchecked: on a null slot it
// returns the sentinel date in 1677; check Valid first. UTC matches the
// bulk reader (QdbTimespecToTime), not the local-time legacy GetTimestamp.
func (c *TimestampColumn) Time(i int) time.Time {
	return time.Unix(0, c.Nanos[i]).UTC()
}

func (c *TimestampColumn) sealed() {}

// appendCell writes row i from cell. A none cell leaves the bit clear and
// stores math.MinInt64. A typed cell outside the int64 nanosecond range is
// ErrOutOfBounds naming the column and row.
func (c *TimestampColumn) appendCell(i int, cell *C.qdb_point_result_t) error {
	if cell._type == C.qdb_query_result_none {
		// The null timespec (qdb_min_time in both fields) never reaches
		// cellNanos: a none cell is written straight as the nanos sentinel.
		c.Nanos[i] = math.MinInt64

		return nil
	}

	sec, nsec := cellTimespec(cell)
	nanos, ok := cellNanos(sec, nsec)
	if !ok {
		return wrapError(C.qdb_e_out_of_bounds, "query_table_timestamp",
			"column", c.name, "row", i, "tv_sec", sec, "tv_nsec", nsec)
	}

	c.Nanos[i] = nanos
	c.valid.set(i)

	return nil
}

// varBytes is the shared body of StringColumn and BlobColumn. bytes holds
// every cell back to back and offsets, of length n+1, bounds cell i as
// bytes[offsets[i]:offsets[i+1]]. Offsets are int32, so a column whose
// bytes exceed math.MaxInt32 is rejected at conversion. Null and empty
// cells both have zero length; only the bitmap tells them apart.
type varBytes struct {
	name    string
	offsets []int32
	bytes   []byte
	valid   Bitmap
}

func newVarBytes(name string, n, nbytes int) varBytes {
	return varBytes{
		name:    name,
		offsets: make([]int32, n+1),
		bytes:   make([]byte, nbytes),
		valid:   newBitmap(n),
	}
}

// Name returns the column name.
func (v *varBytes) Name() string {
	return v.name
}

// Len returns the number of rows.
func (v *varBytes) Len() int {
	return len(v.offsets) - 1
}

// Valid returns the validity bitmap.
func (v *varBytes) Valid() Bitmap {
	return v.valid
}

// Offsets returns the cell boundaries, length Len()+1, without copying:
// cell i spans Bytes()[Offsets()[i]:Offsets()[i+1]]. Read-only; a write
// through it corrupts every string the column has handed out.
func (v *varBytes) Offsets() []int32 {
	return v.offsets
}

// Bytes returns the concatenated cell bytes without copying. Read-only.
func (v *varBytes) Bytes() []byte {
	return v.bytes
}

func (v *varBytes) sealed() {}

// appendCell copies cell i into the shared buffer at the running offset and
// records where it ends. A none cell and an empty cell both advance by
// zero; only the bitmap tells them apart, so the bit comes from the tag
// alone. The tag is checked before the payload is touched because a none
// cell's payload is unspecified and may hold a stale pointer and length.
func (v *varBytes) appendCell(i int, cell *C.qdb_point_result_t) {
	start := v.offsets[i]
	if cell._type == C.qdb_query_result_none {
		v.offsets[i+1] = start

		return
	}

	// The buffer was sized by varSizes from these same lengths, so the
	// copy always fits and the narrowed offset is below math.MaxInt32.
	src := cellBytes(cell)
	copy(v.bytes[start:], src)
	v.offsets[i+1] = start + int32(len(src))
	v.valid.set(i)
}

// StringColumn holds string and symbol cells; see varBytes for the
// buffers. Value returns each cell as a string that aliases the shared
// buffer, so reading a column allocates nothing.
type StringColumn struct{ varBytes }

func newStringColumn(name string, n, nbytes int) *StringColumn {
	return &StringColumn{varBytes: newVarBytes(name, n, nbytes)}
}

// Value returns row i without copying. Unchecked: a null slot yields ""
// exactly as an empty cell does; check Valid first.
//
// The unsafe.String view is sound because the column owns bytes, never
// writes to it after construction, and Bytes documents the buffer as
// read-only, so the immutability Go assumes of a string holds.
func (c *StringColumn) Value(i int) string {
	a, b := c.offsets[i], c.offsets[i+1]
	// An empty cell at the end of the column has a == len(bytes), where
	// taking an element address would panic; "" needs no pointer anyway.
	if a == b {
		return ""
	}

	return unsafe.String(&c.bytes[a], b-a) //nolint:gosec // Justified: bytes is owned, never written after construction, and a < b <= len(bytes)
}

// BlobColumn holds blob cells; see varBytes for the buffers.
type BlobColumn struct{ varBytes }

func newBlobColumn(name string, n, nbytes int) *BlobColumn {
	return &BlobColumn{varBytes: newVarBytes(name, n, nbytes)}
}

// Value returns row i as a view over the shared buffer. Its capacity is
// capped at the cell end, so an append by the caller reallocates instead of
// overwriting the next cell. Unchecked: a null slot yields an empty slice;
// check Valid first. The bytes are read-only.
func (c *BlobColumn) Value(i int) []byte {
	a, b := c.offsets[i], c.offsets[i+1]

	return c.bytes[a:b:b]
}

// NullColumn is a column whose every cell is null: the query produced only
// none cells, so no type can be inferred. It mirrors the C API, which
// carries no column types and encodes null only as a none cell, so the
// column carries a length and nothing else.
type NullColumn struct {
	name  string
	valid Bitmap
}

func newNullColumn(name string, n int) *NullColumn {
	return &NullColumn{name: name, valid: newBitmap(n)}
}

// Name returns the column name.
func (c *NullColumn) Name() string {
	return c.name
}

// Len returns the number of rows.
func (c *NullColumn) Len() int {
	return c.valid.Len()
}

// Valid returns the validity bitmap, which has every bit clear.
func (c *NullColumn) Valid() Bitmap {
	return c.valid
}

func (c *NullColumn) sealed() {}

// QueryTable is a query result copied into Go memory: one QueryColumn per
// result column, all of equal length, plus the scanned point count. It
// holds no C pointers and needs no Close; the QueryResult it was built from
// may be closed as soon as the table exists.
type QueryTable struct {
	columns       []QueryColumn
	byName        map[string]int
	rowCount      int
	scannedPoints int64
}

func newQueryTable(cols []QueryColumn, rowCount int, scanned int64) *QueryTable {
	byName := make(map[string]int, len(cols))
	for i, c := range cols {
		// SQL allows duplicate output names (select a, a). Lookup by name
		// returns the first; Columns still carries every column in order.
		if _, seen := byName[c.Name()]; !seen {
			byName[c.Name()] = i
		}
	}

	return &QueryTable{columns: cols, byName: byName, rowCount: rowCount, scannedPoints: scanned}
}

// RowCount returns the number of rows, the length of every column.
func (t *QueryTable) RowCount() int {
	return t.rowCount
}

// ScannedPoints returns the number of points the server scanned to produce
// the result; the actual number may be greater.
func (t *QueryTable) ScannedPoints() int64 {
	return t.scannedPoints
}

// Columns returns every column in result order, duplicates included,
// without copying. Read-only.
func (t *QueryTable) Columns() []QueryColumn {
	return t.columns
}

// Column returns the column called name, or false when none has that name.
// With duplicate names the first occurrence is returned.
func (t *QueryTable) Column(name string) (QueryColumn, bool) { //nolint:ireturn // Justified: the concrete type is the caller's to discover
	i, ok := t.byName[name]
	if !ok {
		return nil, false
	}

	return t.columns[i], true
}

// ColumnOf returns the column called name as the concrete type T, for
// example ColumnOf[*DoubleColumn](tbl, "price"). A missing column is
// ErrElementNotFound; a column of another type is ErrIncompatibleType with
// both type names in the message.
func ColumnOf[T QueryColumn](tbl *QueryTable, name string) (T, error) { //nolint:ireturn // Justified: T is the caller's concrete type, only the constraint is an interface
	var zero T
	// A missing column and a wrong type are different caller mistakes and
	// get different codes, so the lookup runs before the assertion.
	col, ok := tbl.Column(name)
	if !ok {
		return zero, wrapError(C.qdb_e_element_not_found, "query_table_column_of", "column", name)
	}

	typed, ok := col.(T)
	if !ok {
		// zero is a typed nil pointer, so reflect still names the type.
		return zero, wrapError(C.qdb_e_incompatible_type, "query_table_column_of",
			"column", name, "requested", reflect.TypeOf(zero).String(), "actual", reflect.TypeOf(col).String())
	}

	return typed, nil
}
