package qdb

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fillVarBytes writes cells into a var-width column the way the converter
// does, so accessor tests do not depend on the converter.
func fillVarBytes(v *varBytes, cells [][]byte, valid []bool) {
	off := int32(0)
	for i, cell := range cells {
		copy(v.bytes[off:], cell)
		off += int32(len(cell)) //nolint:gosec // Justified: test cells are a few bytes
		v.offsets[i+1] = off
		if valid[i] {
			v.valid.set(i)
		}
	}
}

func TestQueryTableFixedColumnsExposeBuffers(t *testing.T) {
	i64 := newInt64Column("i", 3)
	i64.Values[1] = 42
	i64.valid.set(1)
	assert.Equal(t, "i", i64.Name())
	assert.Equal(t, 3, i64.Len())
	assert.Equal(t, 2, i64.Valid().NullCount())
	assert.True(t, i64.Valid().IsValid(1))

	dbl := newDoubleColumn("d", 2)
	dbl.Values[0] = 1.5
	assert.Equal(t, "d", dbl.Name())
	assert.Equal(t, 2, dbl.Len())
	assert.True(t, dbl.Valid().AllNull())

	ts := newTimestampColumn("t", 1)
	ts.Nanos[0] = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()
	assert.Equal(t, "t", ts.Name())
	assert.Equal(t, 1, ts.Len())
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC), ts.Time(0))
	assert.Equal(t, time.UTC, ts.Time(0).Location())
}

func TestQueryTableStringColumnValueAliasesBuffer(t *testing.T) {
	cells := [][]byte{[]byte("ab"), nil, []byte("cde"), nil}
	valid := []bool{true, true, true, false}
	col := newStringColumn("s", len(cells), 5)
	fillVarBytes(&col.varBytes, cells, valid)

	assert.Equal(t, "s", col.Name())
	assert.Equal(t, 4, col.Len())
	assert.Equal(t, "ab", col.Value(0))
	assert.Equal(t, "", col.Value(1), "empty cell")
	assert.Equal(t, "cde", col.Value(2))
	assert.Equal(t, "", col.Value(3), "null cell at the end of the buffer")
	assert.Equal(t, []int32{0, 2, 2, 5, 5}, col.Offsets())
	assert.Equal(t, []byte("abcde"), col.Bytes())
	assert.Equal(t, 1, col.Valid().NullCount())
}

func TestQueryTableBlobColumnValueCapsCapacity(t *testing.T) {
	cells := [][]byte{[]byte("ab"), []byte("cd")}
	col := newBlobColumn("b", len(cells), 4)
	fillVarBytes(&col.varBytes, cells, []bool{true, true})

	first := col.Value(0)
	require.Equal(t, []byte("ab"), first)
	require.Equal(t, 2, cap(first))

	// An append must reallocate rather than write into the next cell.
	_ = append(first, 'x') //nolint:staticcheck // the side effect on the buffer is what is under test
	assert.Equal(t, []byte("cd"), col.Value(1))
	assert.Equal(t, []byte("abcd"), col.Bytes())
}

func TestQueryTableNullColumn(t *testing.T) {
	col := newNullColumn("n", 5)
	assert.Equal(t, "n", col.Name())
	assert.Equal(t, 5, col.Len())
	assert.True(t, col.Valid().AllNull())
	assert.Equal(t, 5, col.Valid().NullCount())
}

func TestQueryTableLookupReturnsFirstDuplicate(t *testing.T) {
	first := newInt64Column("a", 1)
	second := newDoubleColumn("a", 1)
	other := newNullColumn("b", 1)
	tbl := newQueryTable([]QueryColumn{first, second, other}, 1, 7)

	assert.Equal(t, 1, tbl.RowCount())
	assert.Equal(t, int64(7), tbl.ScannedPoints())
	assert.Len(t, tbl.Columns(), 3)

	col, ok := tbl.Column("a")
	require.True(t, ok)
	assert.Same(t, first, col)

	col, ok = tbl.Column("b")
	require.True(t, ok)
	assert.Same(t, other, col)

	_, ok = tbl.Column("missing")
	assert.False(t, ok)
}

func TestColumnOfReturnsConcreteType(t *testing.T) {
	price := newDoubleColumn("price", 2)
	tbl := newQueryTable([]QueryColumn{newInt64Column("qty", 2), price}, 2, 0)

	col, err := ColumnOf[*DoubleColumn](tbl, "price")
	require.NoError(t, err)
	assert.Same(t, price, col)
}

func TestColumnOfUnknownNameIsElementNotFound(t *testing.T) {
	tbl := newQueryTable([]QueryColumn{newInt64Column("qty", 1)}, 1, 0)

	col, err := ColumnOf[*Int64Column](tbl, "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrElementNotFound), err.Error())
	assert.Nil(t, col)
	assert.Contains(t, err.Error(), "missing")
}

func TestColumnOfWrongTypeIsIncompatibleType(t *testing.T) {
	tbl := newQueryTable([]QueryColumn{newInt64Column("qty", 1)}, 1, 0)

	col, err := ColumnOf[*StringColumn](tbl, "qty")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleType), err.Error())
	assert.Nil(t, col)
	assert.Contains(t, err.Error(), "*qdb.StringColumn")
	assert.Contains(t, err.Error(), "*qdb.Int64Column")
}

// TestPointRowsFixtureReadsBackThroughAccessors checks the hand-built
// rows through the legacy per-cell accessors, so the converter tests that
// follow rest on a fixture whose layout is known good.
func TestPointRowsFixtureReadsBackThroughAccessors(t *testing.T) {
	handle := newTestHandle(t)
	tags := testArrayTags()
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{
			testCellInt64(7), testCellDouble(1.5), testCellString("abc"), testCellBlob([]byte{1, 2}),
			testCellTimestamp(10, 20), testCellCount(3), testCellNone(), testCellTagged(tags[0]),
		},
		{
			testCellInt64(-1), testCellDouble(math.NaN()), testCellString(""), testCellBlob(nil),
			testCellTimestamp(-5, 0), testCellCount(0), testCellNone(), testCellTagged(tags[4]),
		},
	})
	require.Len(t, rows, 2)

	first := queryPointArrayToSlice(rows[0], 8)
	v, err := first[0].GetInt64()
	require.NoError(t, err)
	assert.Equal(t, int64(7), v)
	d, err := first[1].GetDouble()
	require.NoError(t, err)
	assert.InDelta(t, 1.5, d, 0)
	s, err := first[2].GetString()
	require.NoError(t, err)
	assert.Equal(t, "abc", s)
	b, err := first[3].GetBlob()
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2}, b)
	ts, err := first[4].GetTimestamp()
	require.NoError(t, err)
	assert.Equal(t, time.Unix(10, 20), ts)
	n, err := first[5].GetCount()
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, QueryResultNone, first[6].Get().Type())
	assert.Equal(t, QueryResultValueType(tags[0]), first[7].Get().Type())

	second := queryPointArrayToSlice(rows[1], 8)
	v, err = second[0].GetInt64()
	require.NoError(t, err)
	assert.Equal(t, int64(-1), v)
	d, err = second[1].GetDouble()
	require.NoError(t, err)
	assert.True(t, math.IsNaN(d))
	s, err = second[2].GetString()
	require.NoError(t, err)
	assert.Equal(t, "", s)
	b, err = second[3].GetBlob()
	require.NoError(t, err)
	assert.Empty(t, b)
	ts, err = second[4].GetTimestamp()
	require.NoError(t, err)
	assert.Equal(t, time.Unix(-5, 0), ts)
	assert.Equal(t, QueryResultValueType(tags[4]), second[7].Get().Type())
}

func TestMergeKindNoneIsIdentity(t *testing.T) {
	all := []columnKind{kindNone, kindInt64, kindDouble, kindTimestamp, kindString, kindBlob}
	for _, k := range all {
		got, ok := mergeKind(kindNone, k)
		require.True(t, ok)
		assert.Equal(t, k, got)

		got, ok = mergeKind(k, kindNone)
		require.True(t, ok)
		assert.Equal(t, k, got)

		got, ok = mergeKind(k, k)
		require.True(t, ok)
		assert.Equal(t, k, got)
	}

	for _, a := range all[1:] {
		for _, b := range all[1:] {
			if a == b {
				continue
			}
			_, ok := mergeKind(a, b)
			assert.False(t, ok, "%v + %v", a, b)
		}
	}
}

func TestProbeKindsInfersEachColumn(t *testing.T) {
	handle := newTestHandle(t)
	names := []string{"i", "d", "t", "s", "b", "n", "c"}
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellCount(1)},
		{testCellInt64(1), testCellDouble(1), testCellTimestamp(1, 0), testCellString("x"), testCellBlob([]byte{1}), testCellNone(), testCellInt64(2)},
		{testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone()},
	})

	kinds, err := probeKinds(rows, names)
	require.NoError(t, err)
	assert.Equal(t, []columnKind{kindInt64, kindDouble, kindTimestamp, kindString, kindBlob, kindNone, kindInt64}, kinds)
}

func TestProbeKindsNoRowsIsAllNone(t *testing.T) {
	kinds, err := probeKinds(QueryRows{}, []string{"a", "b"})
	require.NoError(t, err)
	assert.Equal(t, []columnKind{kindNone, kindNone}, kinds)
}

func TestProbeKindsMixedTypesIsIncompatibleType(t *testing.T) {
	handle := newTestHandle(t)
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{testCellInt64(1)},
		{testCellDouble(1)},
	})

	_, err := probeKinds(rows, []string{"mixed"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrIncompatibleType), err.Error())
	assert.Contains(t, err.Error(), "mixed")
	assert.Contains(t, err.Error(), "int64")
	assert.Contains(t, err.Error(), "double")
}

func TestProbeKindsArrayTagsAreNotImplemented(t *testing.T) {
	handle := newTestHandle(t)
	for _, tag := range testArrayTags() {
		rows := newTestPointRows(t, handle, [][]testCellFunc{{testCellTagged(tag)}})

		_, err := probeKinds(rows, []string{"arr"})
		require.Error(t, err, "tag %d", tag)
		assert.True(t, errors.Is(err, ErrNotImplemented), err.Error())
		assert.Contains(t, err.Error(), "arr")
	}
}

// TestQueryTableCellLayout pins the offsets the payload loads depend on;
// the sizes are pinned at compile time in query_table_convert.go.
func TestQueryTableCellLayout(t *testing.T) {
	var cell QueryPoint
	var ts Timespec
	assert.Equal(t, uintptr(24), unsafe.Sizeof(cell))
	assert.Equal(t, uintptr(0), unsafe.Offsetof(cell._type))
	assert.Equal(t, uintptr(8), unsafe.Offsetof(cell.payload))
	assert.Equal(t, uintptr(16), unsafe.Sizeof(cell.payload))
	assert.Equal(t, uintptr(0), unsafe.Offsetof(ts.tv_sec))
	assert.Equal(t, uintptr(8), unsafe.Offsetof(ts.tv_nsec))
}

func TestCellNanosBounds(t *testing.T) {
	cases := []struct {
		sec, nsec int64
		want      int64
		ok        bool
	}{
		{0, 0, 0, true},
		{1, 5, 1_000_000_005, true},
		{-1, 0, -1_000_000_000, true},
		{maxTimespecSec, 854_775_807, math.MaxInt64, true},
		{maxTimespecSec, 854_775_808, 0, false},
		{maxTimespecSec + 1, 0, 0, false},
		{minTimespecSec, -854_775_808, math.MinInt64, true},
		{minTimespecSec, -854_775_809, 0, false},
		{minTimespecSec - 1, 0, 0, false},
		{math.MinInt64, math.MinInt64, 0, false},
	}
	for _, c := range cases {
		got, ok := cellNanos(c.sec, c.nsec)
		assert.Equal(t, c.ok, ok, "sec=%d nsec=%d", c.sec, c.nsec)
		if c.ok {
			assert.Equal(t, c.want, got, "sec=%d nsec=%d", c.sec, c.nsec)
		}
	}
}

func TestAppendRowFillsFixedColumns(t *testing.T) {
	handle := newTestHandle(t)
	names := []string{"i", "d", "t", "c", "n"}
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{testCellInt64(7), testCellDouble(1.5), testCellTimestamp(10, 20), testCellCount(3), testCellNone()},
		{testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone()},
		{testCellInt64(-7), testCellDouble(-1.5), testCellTimestamp(-10, 20), testCellInt64(4), testCellNone()},
	})
	kinds, err := probeKinds(rows, names)
	require.NoError(t, err)
	cols := allocColumns(names, kinds, len(rows), nil)
	for i, row := range rows {
		require.NoError(t, appendRow(cols, row, i))
	}

	i64, err := ColumnOf[*Int64Column](newQueryTable(cols, 3, 0), "i")
	require.NoError(t, err)
	assert.Equal(t, []int64{7, math.MinInt64, -7}, i64.Values)
	assert.Equal(t, []bool{true, false, true}, validBits(i64.Valid()))

	dbl, err := ColumnOf[*DoubleColumn](newQueryTable(cols, 3, 0), "d")
	require.NoError(t, err)
	assert.InDelta(t, 1.5, dbl.Values[0], 0)
	assert.True(t, math.IsNaN(dbl.Values[1]))
	assert.InDelta(t, -1.5, dbl.Values[2], 0)
	assert.Equal(t, []bool{true, false, true}, validBits(dbl.Valid()))

	ts, err := ColumnOf[*TimestampColumn](newQueryTable(cols, 3, 0), "t")
	require.NoError(t, err)
	assert.Equal(t, []int64{10_000_000_020, math.MinInt64, -9_999_999_980}, ts.Nanos)
	assert.Equal(t, time.Unix(10, 20).UTC(), ts.Time(0))
	assert.Equal(t, []bool{true, false, true}, validBits(ts.Valid()))

	cnt, err := ColumnOf[*Int64Column](newQueryTable(cols, 3, 0), "c")
	require.NoError(t, err)
	assert.Equal(t, []int64{3, math.MinInt64, 4}, cnt.Values)

	null, err := ColumnOf[*NullColumn](newQueryTable(cols, 3, 0), "n")
	require.NoError(t, err)
	assert.Equal(t, 3, null.Len())
	assert.True(t, null.Valid().AllNull())
}

func TestAppendRowTimestampOverflowIsOutOfBounds(t *testing.T) {
	handle := newTestHandle(t)
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{testCellTimestamp(0, 0)},
		{testCellTimestamp(maxTimespecSec+1, 0)},
	})
	cols := allocColumns([]string{"when"}, []columnKind{kindTimestamp}, 2, nil)

	require.NoError(t, appendRow(cols, rows[0], 0))
	err := appendRow(cols, rows[1], 1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOutOfBounds), err.Error())
	assert.Contains(t, err.Error(), "when")
	assert.Contains(t, err.Error(), "row=1")
}

// validBits expands a bitmap to a []bool for equality assertions.
func validBits(b Bitmap) []bool {
	out := make([]bool, b.Len())
	for i := range out {
		out[i] = b.IsValid(i)
	}

	return out
}

func TestAppendRowFillsVarColumns(t *testing.T) {
	handle := newTestHandle(t)
	names := []string{"s", "b"}
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{testCellString("ab"), testCellBlob([]byte{1})},
		{testCellNone(), testCellNone()},
		{testCellString(""), testCellBlob(nil)},
		{testCellSeq(testCellString("stale"), testCellNone()), testCellBlob([]byte{2, 3})},
		{testCellString("cde"), testCellNone()},
	})
	kinds, err := probeKinds(rows, names)
	require.NoError(t, err)
	sizes, err := varSizes(rows, kinds, names)
	require.NoError(t, err)
	assert.Equal(t, []int{5, 3}, sizes, "stale payload under a none tag is not counted")

	cols := allocColumns(names, kinds, len(rows), sizes)
	for i, row := range rows {
		require.NoError(t, appendRow(cols, row, i))
	}
	tbl := newQueryTable(cols, len(rows), 0)

	str, err := ColumnOf[*StringColumn](tbl, "s")
	require.NoError(t, err)
	assert.Equal(t, []string{"ab", "", "", "", "cde"}, []string{str.Value(0), str.Value(1), str.Value(2), str.Value(3), str.Value(4)})
	assert.Equal(t, []int32{0, 2, 2, 2, 2, 5}, str.Offsets())
	assert.Equal(t, []byte("abcde"), str.Bytes())
	assert.Equal(t, []bool{true, false, true, false, true}, validBits(str.Valid()))

	blob, err := ColumnOf[*BlobColumn](tbl, "b")
	require.NoError(t, err)
	assert.Equal(t, []byte{1}, blob.Value(0))
	assert.Empty(t, blob.Value(1))
	assert.Empty(t, blob.Value(2))
	assert.Equal(t, []byte{2, 3}, blob.Value(3))
	assert.Empty(t, blob.Value(4))
	assert.Equal(t, []int32{0, 1, 1, 1, 3, 3}, blob.Offsets())
	assert.Equal(t, []bool{true, false, true, true, false}, validBits(blob.Valid()))
}

func TestVarSizesRejectsColumnPastInt32(t *testing.T) {
	handle := newTestHandle(t)
	rows := newTestPointRows(t, handle, [][]testCellFunc{
		{testCellBlob([]byte{1})},
		{testCellBlobUnbacked(math.MaxInt32)},
	})

	_, err := varSizes(rows, []columnKind{kindBlob}, []string{"big"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOutOfBounds), err.Error())
	assert.Contains(t, err.Error(), "big")

	sizes, err := varSizes(rows[:1], []columnKind{kindBlob}, []string{"big"})
	require.NoError(t, err)
	assert.Equal(t, []int{1}, sizes)
}

// Column positions of a select * over the all-columns fixture: the two
// system columns come first, then the fixture's columns in creation order.
const (
	tblTimestampIndex = 0
	tblTableIndex     = 1
	tblBlobIndex      = 2
	tblDoubleIndex    = 3
	tblInt64Index     = 4
	tblStringIndex    = 5
	tblTsIndex        = 6
	tblSymbolIndex    = 7
)

// toTable executes query, converts it, and closes the result.
func toTable(t *testing.T, handle HandleType, query string) *QueryTable {
	t.Helper()

	result, err := handle.Query(query).Execute()
	require.NoError(t, err)
	defer result.Close()

	tbl, err := result.ToTable()
	require.NoError(t, err)

	return tbl
}

// fixtureTable converts a select * over the fixture and returns the table
// with the result's column names.
func fixtureTable(t *testing.T, handle HandleType, td TestTimeseriesData) (tbl *QueryTable, names []string) {
	t.Helper()

	result, err := handle.Query(fmt.Sprintf("select * from %s in range(1970, +10d)", td.Alias)).Execute()
	require.NoError(t, err)
	defer result.Close()

	tbl, err = result.ToTable()
	require.NoError(t, err)

	return tbl, result.ColumnsNames()
}

func requireTableMatchesFixture(t *testing.T, tbl *QueryTable, td TestTimeseriesData) {
	t.Helper()

	n := len(td.Int64Points)
	require.Equal(t, n, tbl.RowCount())
	require.Len(t, tbl.Columns(), 8)

	idx, ok := tbl.Columns()[tblTimestampIndex].(*TimestampColumn)
	require.True(t, ok)
	table, ok := tbl.Columns()[tblTableIndex].(*StringColumn)
	require.True(t, ok)
	blob, ok := tbl.Columns()[tblBlobIndex].(*BlobColumn)
	require.True(t, ok)
	dbl, ok := tbl.Columns()[tblDoubleIndex].(*DoubleColumn)
	require.True(t, ok)
	i64, ok := tbl.Columns()[tblInt64Index].(*Int64Column)
	require.True(t, ok)
	str, ok := tbl.Columns()[tblStringIndex].(*StringColumn)
	require.True(t, ok)
	ts, ok := tbl.Columns()[tblTsIndex].(*TimestampColumn)
	require.True(t, ok)
	sym, ok := tbl.Columns()[tblSymbolIndex].(*StringColumn)
	require.True(t, ok)

	assert.True(t, idx.Valid().AllValid())
	assert.True(t, table.Valid().AllValid())
	assert.Equal(t, td.BlobValid, validBits(blob.Valid()))
	assert.Equal(t, td.DoubleValid, validBits(dbl.Valid()))
	assert.Equal(t, td.Int64Valid, validBits(i64.Valid()))
	assert.Equal(t, td.StringValid, validBits(str.Valid()))
	assert.Equal(t, td.TimestampValid, validBits(ts.Valid()))
	assert.Equal(t, td.SymbolValid, validBits(sym.Valid()))

	for i := range n {
		assert.Equal(t, td.Int64Points[i].Timestamp().UnixNano(), idx.Nanos[i], "row %d", i)
		assert.Equal(t, td.Alias, table.Value(i), "row %d", i)
		requireCellMatches(t, td, i, blob, dbl, i64, str, ts, sym)
	}
}

// requireCellMatches checks row i: the fixture value on a valid slot, the
// null sentinel on a cleared one.
func requireCellMatches(t *testing.T, td TestTimeseriesData, i int,
	blob *BlobColumn, dbl *DoubleColumn, i64 *Int64Column, str *StringColumn, ts *TimestampColumn, sym *StringColumn,
) {
	t.Helper()

	if td.BlobValid[i] {
		assert.Equal(t, td.BlobPoints[i].Content(), blob.Value(i), "row %d", i)
	} else {
		assert.Empty(t, blob.Value(i), "row %d", i)
	}
	if td.DoubleValid[i] {
		assert.InDelta(t, td.DoublePoints[i].Content(), dbl.Values[i], 0, "row %d", i)
	} else {
		assert.True(t, math.IsNaN(dbl.Values[i]), "row %d", i)
	}
	if td.Int64Valid[i] {
		assert.Equal(t, td.Int64Points[i].Content(), i64.Values[i], "row %d", i)
	} else {
		assert.Equal(t, int64(math.MinInt64), i64.Values[i], "row %d", i)
	}
	if td.StringValid[i] {
		assert.Equal(t, td.StringPoints[i].Content(), str.Value(i), "row %d", i)
	} else {
		assert.Equal(t, "", str.Value(i), "row %d", i)
	}
	if td.TimestampValid[i] {
		assert.Equal(t, td.TimestampPoints[i].Content().UnixNano(), ts.Nanos[i], "row %d", i)
		assert.True(t, td.TimestampPoints[i].Content().Equal(ts.Time(i)), "row %d", i)
	} else {
		assert.Equal(t, int64(math.MinInt64), ts.Nanos[i], "row %d", i)
	}
	if td.SymbolValid[i] {
		assert.Equal(t, td.SymbolPoints[i].Content(), sym.Value(i), "row %d", i)
	} else {
		assert.Equal(t, "", sym.Value(i), "row %d", i)
	}
}

func TestToTableDenseFixture(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumns(t, handle, 8)

	tbl, names := fixtureTable(t, handle, td)
	assert.Len(t, names, 8)
	assert.Positive(t, tbl.ScannedPoints())
	for i, c := range tbl.Columns() {
		assert.Equal(t, names[i], c.Name())
	}
	requireTableMatchesFixture(t, tbl, td)
}

func TestToTableSparseFixture(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 64, 50)

	tbl, _ := fixtureTable(t, handle, td)
	requireTableMatchesFixture(t, tbl, td)
}

func TestToTableAllNullFixtureYieldsNullColumns(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 4, 0)

	tbl, _ := fixtureTable(t, handle, td)
	require.Equal(t, 4, tbl.RowCount())
	require.Len(t, tbl.Columns(), 8)

	_, ok := tbl.Columns()[tblTimestampIndex].(*TimestampColumn)
	assert.True(t, ok, "$timestamp stays typed")
	_, ok = tbl.Columns()[tblTableIndex].(*StringColumn)
	assert.True(t, ok, "$table stays typed")
	for _, c := range tbl.Columns()[tblBlobIndex:] {
		null, ok := c.(*NullColumn)
		require.True(t, ok, "column %s is %T", c.Name(), c)
		assert.Equal(t, 4, null.Len())
		assert.True(t, null.Valid().AllNull())
	}
}

func TestToTableCountAggregateIsInt64Column(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 16, 50)
	_, names := fixtureTable(t, handle, td)

	tbl := toTable(t, handle, fmt.Sprintf("select count(%s) from %s in range(1970, +10d)", names[tblInt64Index], td.Alias))
	require.Equal(t, 1, tbl.RowCount())
	require.Len(t, tbl.Columns(), 1)

	cnt, err := ColumnOf[*Int64Column](tbl, tbl.Columns()[0].Name())
	require.NoError(t, err)
	valid := 0
	for _, ok := range td.Int64Valid {
		if ok {
			valid++
		}
	}
	assert.Equal(t, []int64{int64(valid)}, cnt.Values)
	assert.True(t, cnt.Valid().AllValid())
}

func TestToTableEmptyRangeHasNoRows(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumns(t, handle, 4)

	result, err := handle.Query(fmt.Sprintf("select * from %s in range(1971, +10d)", td.Alias)).Execute()
	require.NoError(t, err)
	defer result.Close()

	tbl, err := result.ToTable()
	require.NoError(t, err)
	assert.Equal(t, 0, tbl.RowCount())
	assert.Len(t, tbl.Columns(), int(result.ColumnsCount()))
	for _, c := range tbl.Columns() {
		assert.Equal(t, 0, c.Len())
	}
}

func TestToTableOnClosedOrNilResultIsEmpty(t *testing.T) {
	var nilResult *QueryResult
	tbl, err := nilResult.ToTable()
	require.NoError(t, err)
	assert.Equal(t, 0, tbl.RowCount())
	assert.Empty(t, tbl.Columns())

	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumns(t, handle, 2)
	result, err := handle.Query(fmt.Sprintf("select * from %s in range(1970, +10d)", td.Alias)).Execute()
	require.NoError(t, err)
	result.Close()

	tbl, err = result.ToTable()
	require.NoError(t, err)
	assert.Equal(t, 0, tbl.RowCount())
	assert.Empty(t, tbl.Columns())
}

// TestToTableOutlivesResult proves the table aliases no C memory: the
// result is released and the heap scrubbed before the values are read.
func TestToTableOutlivesResult(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 32, 50)

	result, err := handle.Query(fmt.Sprintf("select * from %s in range(1970, +10d)", td.Alias)).Execute()
	require.NoError(t, err)
	tbl, err := result.ToTable()
	require.NoError(t, err)
	result.Close()

	runtime.GC()
	debug.FreeOSMemory()
	requireTableMatchesFixture(t, tbl, td)
}
