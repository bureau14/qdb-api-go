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

// QueryResult holds the rows returned by Query.Execute. The underlying buffer
// is API-allocated and lives until Close is called or the handle is closed;
// callers must call Close. Accessors return zero values once closed.
type QueryResult struct {
	// Handle the query was executed on; qdb_release must use the same handle.
	handle HandleType

	// API-allocated result set. Nil once closed, or when the statement
	// produced no result set (e.g. DDL).
	result *C.qdb_query_result_t
}

// Close releases the API-allocated result buffer. Safe to call on a nil
// receiver and more than once. Rows, columns and points obtained from this
// result must not be used after Close.
func (r *QueryResult) Close() {
	if r == nil || r.result == nil {
		return
	}

	qdbReleasePointer(r.handle, unsafe.Pointer(r.result))
	// Barrier-free nil store; see setCPtr.
	setCPtr(unsafe.Pointer(&r.result), nil)
}

// ScannedPoints : number of points scanned
//
//	The actual number of scanned points may be greater
func (r QueryResult) ScannedPoints() int64 {
	if r.result == nil {
		return 0
	}

	return int64(r.result.scanned_point_count)
}

// queryPointArrayToSlice views length contiguous cells starting at row as a
// slice, without copying. The view is valid until the result is closed.
func queryPointArrayToSlice(row *QueryPoint, length int64) []QueryPoint {
	return unsafe.Slice(row, length)
}

// qdbPointResultStarArrayToSlice views the array of length row pointers as
// a slice, without copying. The view is valid until the result is closed.
func qdbPointResultStarArrayToSlice(rows **C.qdb_point_result_t, length int64) []*QueryPoint {
	return unsafe.Slice((**QueryPoint)(unsafe.Pointer(rows)), length)
}

// qdbStringArrayToSlice views the array of length column names as a slice,
// without copying. The view is valid until the result is closed.
func qdbStringArrayToSlice(strings *C.qdb_string_t, length int64) []C.qdb_string_t {
	return unsafe.Slice(strings, length)
}

// Columns : create columns from a row
func (r QueryResult) Columns(row *QueryPoint) QueryRow {
	return r.columnsUnsafe(row)
}

// Rows : get rows of a query table result
func (r QueryResult) Rows() QueryRows {
	return r.rowsUnsafe()
}

// ColumnsNames : get the number of columns names of each row
func (r QueryResult) ColumnsNames() []string {
	if r.result == nil {
		return []string{}
	}

	count := int64(r.result.column_count)
	result := make([]string, count)
	rawNames := qdbStringArrayToSlice(r.result.column_names, count)
	for i := range rawNames {
		result[i] = C.GoString(rawNames[i].data)
	}

	return result
}

// ColumnsCount : get the number of columns of each row
func (r QueryResult) ColumnsCount() int64 {
	if r.result == nil {
		return 0
	}

	return int64(r.result.column_count)
}

// RowCount : the number of returned rows
func (r QueryResult) RowCount() int64 {
	if r.result == nil {
		return 0
	}

	return int64(r.result.row_count)
}

// ErrorMessage : the error message in case of failure
func (r QueryResult) ErrorMessage() string {
	if r.result == nil {
		return ""
	}

	return C.GoStringN(r.result.error_message.data, C.int(r.result.error_message.length))
}

// columnsUnsafe views the cells of row in place. The slice aliases the C
// result and is valid only until Close.
func (r QueryResult) columnsUnsafe(row *QueryPoint) QueryRow {
	if r.result == nil {
		return QueryRow{}
	}

	count := int64(r.result.column_count)

	return queryPointArrayToSlice(row, count)
}

// rowsUnsafe views the row pointers in place. The slice aliases the C
// result and is valid only until Close.
func (r QueryResult) rowsUnsafe() QueryRows {
	if r.result == nil {
		return QueryRows{}
	}

	count := int64(r.result.row_count)
	if count == 0 {
		return []*QueryPoint{}
	}

	return qdbPointResultStarArrayToSlice(r.result.rows, count)
}

// QueryColumn is one column of a QueryResultSet: a named masked array whose
// concrete types are closed to this package, dispatched with a type switch
// or ColumnOf. Buffers are exposed directly and are read-only. Accessors
// never consult the mask; a null slot holds the null sentinel of its type.
type QueryColumn interface {
	// Name is the column name as reported by the query, duplicates included.
	Name() string
	// Len is the number of rows.
	Len() int
	// Valid is the mask: bit i set means row i holds a value.
	Valid() Mask
	sealed()
}

// MaskedArray is a dense value slice with a Mask: Mask bit i set means
// Values[i] holds a value, clear means the slot is null and holds the null
// sentinel of the column type, so Values can be handed to the batch writer
// as it is and the nulls are written back as nulls.
type MaskedArray[T any] struct {
	Values []T
	Mask   Mask
}

func newMaskedArray[T any](n int) MaskedArray[T] {
	return MaskedArray[T]{Values: make([]T, n), Mask: newMask(n)}
}

// Len returns the number of rows.
func (a *MaskedArray[T]) Len() int {
	return len(a.Values)
}

// Valid returns the mask. It exists so the mask is reachable through the
// QueryColumn interface; on a concrete column the Mask field is the same.
func (a *MaskedArray[T]) Valid() Mask {
	return a.Mask
}

// QueryColumnInt64 holds int64 cells. A count(...) aggregate also lands
// here, its unsigned payload reinterpreted as int64. Null slots hold
// math.MinInt64, the QDB_IS_NULL_INT64 sentinel.
type QueryColumnInt64 struct {
	MaskedArray[int64]
	name string
}

func newQueryColumnInt64(name string, n int) *QueryColumnInt64 {
	return &QueryColumnInt64{MaskedArray: newMaskedArray[int64](n), name: name}
}

// Name returns the column name.
func (c *QueryColumnInt64) Name() string {
	return c.name
}

func (c *QueryColumnInt64) sealed() {}

// appendCell writes row i from cell. A none cell leaves the bit clear and
// stores the sentinel: the mask is authoritative, the sentinel only keeps
// Values usable by the writer.
func (c *QueryColumnInt64) appendCell(i int, cell *C.qdb_point_result_t) {
	if cell._type == C.qdb_query_result_none {
		c.Values[i] = math.MinInt64

		return
	}

	// int64 and count share this path: pass one accepted both tags for
	// this column, and the count payload is read as the same eight bytes.
	c.Values[i] = cellInt64(cell)
	c.Mask.set(i)
}

// QueryColumnDouble holds double cells. Null slots hold NaN, the
// QDB_IS_NULL_DOUBLE sentinel. Values are IEEE-754 binary64, the C double
// on every supported platform.
type QueryColumnDouble struct {
	MaskedArray[float64]
	name string
}

func newQueryColumnDouble(name string, n int) *QueryColumnDouble {
	return &QueryColumnDouble{MaskedArray: newMaskedArray[float64](n), name: name}
}

// Name returns the column name.
func (c *QueryColumnDouble) Name() string {
	return c.name
}

func (c *QueryColumnDouble) sealed() {}

// appendCell writes row i from cell. A none cell leaves the bit clear and
// stores NaN.
func (c *QueryColumnDouble) appendCell(i int, cell *C.qdb_point_result_t) {
	if cell._type == C.qdb_query_result_none {
		c.Values[i] = math.NaN()

		return
	}

	c.Values[i] = cellDouble(cell)
	c.Mask.set(i)
}

// QueryColumnTimestamp holds timestamp cells as int64 nanoseconds since the
// Unix epoch: one 8-byte value per row with no pointer, so the column
// compares and sorts like any numeric slice. The representable range is
// the years 1678 to 2262; a cell outside it fails conversion with
// ErrOutOfBounds. Null slots hold math.MinInt64.
type QueryColumnTimestamp struct {
	MaskedArray[int64]
	name string
}

func newQueryColumnTimestamp(name string, n int) *QueryColumnTimestamp {
	return &QueryColumnTimestamp{MaskedArray: newMaskedArray[int64](n), name: name}
}

// Name returns the column name.
func (c *QueryColumnTimestamp) Name() string {
	return c.name
}

// Time converts row i to a UTC time.Time. Unchecked: on a null slot it
// returns the sentinel date in 1677; check the mask first. UTC matches the
// bulk reader (QdbTimespecToTime), not the local-time legacy GetTimestamp.
func (c *QueryColumnTimestamp) Time(i int) time.Time {
	return time.Unix(0, c.Values[i]).UTC()
}

func (c *QueryColumnTimestamp) sealed() {}

// appendCell writes row i from cell. A none cell leaves the bit clear and
// stores math.MinInt64. A typed cell outside the int64 nanosecond range is
// ErrOutOfBounds naming the column and row.
func (c *QueryColumnTimestamp) appendCell(i int, cell *C.qdb_point_result_t) error {
	if cell._type == C.qdb_query_result_none {
		// The null timespec (qdb_min_time in both fields) never reaches
		// cellNanos: a none cell is written straight as the nanos sentinel.
		c.Values[i] = math.MinInt64

		return nil
	}

	sec, nsec := cellTimespec(cell)
	nanos, ok := cellNanos(sec, nsec)
	if !ok {
		return wrapError(C.qdb_e_out_of_bounds, "query_result_set_timestamp",
			"column", c.name, "row", i, "tv_sec", sec, "tv_nsec", nsec)
	}

	c.Values[i] = nanos
	c.Mask.set(i)

	return nil
}

// cellBuffer holds the content of every cell of a string or blob column
// back to back in row order, sized exactly by varSizes, and the offset at
// which each cell starts: cell i is bytes[offsets[i]:offsets[i+1]], so
// offsets has one entry more than the column has rows and offsets[0] is 0.
// A column costs two allocations, and each Values[i] is a view into bytes
// rather than a copy. Offsets are int32 rather than int: half the footprint
// of int64 offsets, and the width that lets the pair be handed on to other
// columnar consumers without a conversion pass. varSizes enforces the
// resulting cap on the buffer size.
type cellBuffer struct {
	bytes   []byte
	offsets []int32
}

func newCellBuffer(n, nbytes int) cellBuffer {
	return cellBuffer{bytes: make([]byte, nbytes), offsets: make([]int32, n+1)}
}

// push copies src in as cell i and returns the view over the copy. Cells
// arrive in row order, so offsets[i] is already the end of cell i-1, and
// the end of this cell becomes offsets[i+1]. A null cell pushes nil so its
// two offsets coincide and the sequence has no gaps. The capacity of the
// view ends at the cell, so an append by the caller reallocates instead of
// overwriting the next cell.
func (b *cellBuffer) push(i int, src []byte) []byte {
	start := int(b.offsets[i])
	end := start + copy(b.bytes[start:], src)
	b.offsets[i+1] = int32(end)

	return b.bytes[start:end:end]
}

// QueryColumnString holds string and symbol cells. Every Values[i] aliases
// one shared buffer, so reading a column allocates nothing per cell. Null
// slots hold "", the QDB_IS_NULL_STRING sentinel; only the mask tells a
// null from an empty string.
type QueryColumnString struct {
	MaskedArray[string]
	buf  cellBuffer
	name string
}

func newQueryColumnString(name string, n, nbytes int) *QueryColumnString {
	return &QueryColumnString{
		MaskedArray: newMaskedArray[string](n),
		buf:         newCellBuffer(n, nbytes),
		name:        name,
	}
}

// Name returns the column name.
func (c *QueryColumnString) Name() string {
	return c.name
}

// Bytes returns the shared buffer holding every cell back to back in row
// order, read-only. Cell i is Bytes()[Offsets()[i]:Offsets()[i+1]]; a null
// or empty cell occupies no bytes.
func (c *QueryColumnString) Bytes() []byte {
	return c.buf.bytes
}

// Offsets returns Len()+1 cell boundaries into Bytes, read-only, starting
// at 0 and ending at len(Bytes()). Consecutive equal offsets mark a null
// or empty cell; only the mask tells them apart.
func (c *QueryColumnString) Offsets() []int32 {
	return c.buf.offsets
}

func (c *QueryColumnString) sealed() {}

// appendCell writes row i from cell. The tag is checked before the payload
// is touched because a none cell's payload is unspecified and may hold a
// stale pointer and length.
func (c *QueryColumnString) appendCell(i int, cell *C.qdb_point_result_t) {
	if cell._type == C.qdb_query_result_none {
		// The null cell still takes its turn in the buffer so the offsets
		// stay one per row.
		c.buf.push(i, nil)
		c.Values[i] = ""

		return
	}

	// An empty typed cell is valid and needs no bytes; unsafe.String must
	// not be given the address one past the buffer, which is where an empty
	// last cell would point.
	view := c.buf.push(i, cellBytes(cell))
	if len(view) > 0 {
		// Sound because the buffer is owned by the column, written only
		// here, and never exposed for writing.
		c.Values[i] = unsafe.String(unsafe.SliceData(view), len(view)) //nolint:gosec // Justified: view is a private, write-once buffer
	}
	c.Mask.set(i)
}

// QueryColumnBlob holds blob cells. Every Values[i] is a view into one
// shared buffer with its capacity capped at the cell, so an append by the
// caller reallocates instead of overwriting the next cell. Null slots hold
// nil, the QDB_IS_NULL_BLOB sentinel; an empty typed cell is nil as well,
// and only the mask tells them apart.
type QueryColumnBlob struct {
	MaskedArray[[]byte]
	buf  cellBuffer
	name string
}

func newQueryColumnBlob(name string, n, nbytes int) *QueryColumnBlob {
	return &QueryColumnBlob{
		MaskedArray: newMaskedArray[[]byte](n),
		buf:         newCellBuffer(n, nbytes),
		name:        name,
	}
}

// Name returns the column name.
func (c *QueryColumnBlob) Name() string {
	return c.name
}

// Bytes returns the shared buffer holding every cell back to back in row
// order, read-only. Cell i is Bytes()[Offsets()[i]:Offsets()[i+1]]; a null
// or empty cell occupies no bytes.
func (c *QueryColumnBlob) Bytes() []byte {
	return c.buf.bytes
}

// Offsets returns Len()+1 cell boundaries into Bytes, read-only, starting
// at 0 and ending at len(Bytes()). Consecutive equal offsets mark a null
// or empty cell; only the mask tells them apart.
func (c *QueryColumnBlob) Offsets() []int32 {
	return c.buf.offsets
}

func (c *QueryColumnBlob) sealed() {}

// appendCell writes row i from cell; see QueryColumnString.appendCell.
func (c *QueryColumnBlob) appendCell(i int, cell *C.qdb_point_result_t) {
	if cell._type == C.qdb_query_result_none {
		c.buf.push(i, nil)
		c.Values[i] = nil

		return
	}

	if view := c.buf.push(i, cellBytes(cell)); len(view) > 0 {
		c.Values[i] = view
	}
	c.Mask.set(i)
}

// QueryColumnNull is a column whose every cell is null: the query produced
// only none cells, so no type can be inferred. It mirrors the C API, which
// carries no column types and encodes null only as a none cell, so the
// column carries a length and nothing else.
type QueryColumnNull struct {
	name string
	mask Mask
}

func newQueryColumnNull(name string, n int) *QueryColumnNull {
	return &QueryColumnNull{name: name, mask: newMask(n)}
}

// Name returns the column name.
func (c *QueryColumnNull) Name() string {
	return c.name
}

// Len returns the number of rows.
func (c *QueryColumnNull) Len() int {
	return c.mask.Len()
}

// Valid returns the mask, which has every bit clear.
func (c *QueryColumnNull) Valid() Mask {
	return c.mask
}

func (c *QueryColumnNull) sealed() {}

// QueryResultSet is a query result copied into Go memory: one QueryColumn per
// result column, all of equal length, plus the scanned point count. It
// holds no C pointers and needs no Close; the QueryResult it was built from
// may be closed as soon as the result set exists.
type QueryResultSet struct {
	columns       []QueryColumn
	byName        map[string]int
	rowCount      int
	scannedPoints int64
}

func newQueryResultSet(cols []QueryColumn, rowCount int, scanned int64) *QueryResultSet {
	byName := make(map[string]int, len(cols))
	for i, c := range cols {
		// SQL allows duplicate output names (select a, a). Lookup by name
		// returns the first; Columns still carries every column in order.
		if _, seen := byName[c.Name()]; !seen {
			byName[c.Name()] = i
		}
	}

	return &QueryResultSet{columns: cols, byName: byName, rowCount: rowCount, scannedPoints: scanned}
}

// RowCount returns the number of rows, the length of every column.
func (t *QueryResultSet) RowCount() int {
	return t.rowCount
}

// ScannedPoints returns the number of points the server scanned to produce
// the result; the actual number may be greater.
func (t *QueryResultSet) ScannedPoints() int64 {
	return t.scannedPoints
}

// Columns returns every column in result order, duplicates included,
// without copying. Read-only.
func (t *QueryResultSet) Columns() []QueryColumn {
	return t.columns
}

// Column returns the column called name, or false when none has that name.
// With duplicate names the first occurrence is returned.
func (t *QueryResultSet) Column(name string) (QueryColumn, bool) { //nolint:ireturn // Justified: the concrete type is the caller's to discover
	i, ok := t.byName[name]
	if !ok {
		return nil, false
	}

	return t.columns[i], true
}

// ColumnOf returns the column called name as the concrete type T, for
// example ColumnOf[*QueryColumnDouble](rs, "price"). A missing column is
// ErrElementNotFound; a column of another type is ErrIncompatibleType with
// both type names in the message.
func ColumnOf[T QueryColumn](rs *QueryResultSet, name string) (T, error) { //nolint:ireturn // Justified: T is the caller's concrete type, only the constraint is an interface
	var zero T
	// A missing column and a wrong type are different caller mistakes and
	// get different codes, so the lookup runs before the assertion.
	col, ok := rs.Column(name)
	if !ok {
		return zero, wrapError(C.qdb_e_element_not_found, "query_result_set_column_of", "column", name)
	}

	typed, ok := col.(T)
	if !ok {
		// zero is a typed nil pointer, so reflect still names the type.
		return zero, wrapError(C.qdb_e_incompatible_type, "query_result_set_column_of",
			"column", name, "requested", reflect.TypeOf(zero).String(), "actual", reflect.TypeOf(col).String())
	}

	return typed, nil
}
