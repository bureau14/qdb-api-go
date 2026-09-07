package qdb

import (
	"errors"
	"math"
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
