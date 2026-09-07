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

// Column positions of a select * over the all-columns fixture: the two
// system columns come first, then the fixture's columns in creation order.
const (
	rsTimestampIndex = 0
	rsTableIndex     = 1
	rsBlobIndex      = 2
	rsDoubleIndex    = 3
	rsInt64Index     = 4
	rsStringIndex    = 5
	rsTsIndex        = 6
	rsSymbolIndex    = 7
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

// validBits expands a bitmap to a []bool for equality assertions.
func validBits(b Bitmap) []bool {
	out := make([]bool, b.Len())
	for i := range out {
		out[i] = b.IsValid(i)
	}

	return out
}

func allFalse(mask []bool) bool {
	for _, v := range mask {
		if v {
			return false
		}
	}

	return true
}

func selectFixture(td TestTimeseriesData) string {
	return fmt.Sprintf("select * from %s in range(1970, +10d)", td.Alias)
}

// fixtureColumn asserts column j of rs against the fixture mask: a
// QueryColumnNull when the fixture wrote no value in the column, the concrete
// type T otherwise.
func fixtureColumn[T QueryColumn](t *testing.T, rs *QueryResultSet, j int, mask []bool) (T, bool) { //nolint:ireturn // Justified: T is the caller's concrete type
	t.Helper()

	col := rs.Columns()[j]
	if allFalse(mask) {
		null, ok := col.(*QueryColumnNull)
		require.True(t, ok, "column %s is %T, want QueryColumnNull", col.Name(), col)
		assert.Equal(t, len(mask), null.Len())
		assert.True(t, null.Valid().AllNull())

		var zero T

		return zero, false
	}

	typed, ok := col.(T)
	require.True(t, ok, "column %s is %T", col.Name(), col)
	assert.Equal(t, mask, validBits(typed.Valid()), "column %s", col.Name())

	return typed, true
}

// requireResultSetMatchesFixture checks every column of a select * over the
// fixture: the fixture value on a valid slot, the null sentinel on a
// cleared one, and a QueryColumnNull where the fixture wrote nothing.
func requireResultSetMatchesFixture(t *testing.T, rs *QueryResultSet, td TestTimeseriesData) {
	t.Helper()

	n := len(td.Int64Points)
	require.Equal(t, n, rs.RowCount())
	require.Len(t, rs.Columns(), 8)

	idx, ok := rs.Columns()[rsTimestampIndex].(*QueryColumnTimestamp)
	require.True(t, ok)
	table, ok := rs.Columns()[rsTableIndex].(*QueryColumnString)
	require.True(t, ok)
	assert.True(t, idx.Valid().AllValid())
	assert.True(t, table.Valid().AllValid())
	for i := range n {
		assert.Equal(t, td.Int64Points[i].Timestamp().UnixNano(), idx.Nanos[i], "row %d", i)
		assert.Equal(t, td.Alias, table.Value(i), "row %d", i)
	}

	if blob, ok := fixtureColumn[*QueryColumnBlob](t, rs, rsBlobIndex, td.BlobValid); ok {
		for i, valid := range td.BlobValid {
			if valid {
				assert.Equal(t, td.BlobPoints[i].Content(), blob.Value(i), "row %d", i)
			} else {
				assert.Empty(t, blob.Value(i), "row %d", i)
			}
		}
	}
	if dbl, ok := fixtureColumn[*QueryColumnDouble](t, rs, rsDoubleIndex, td.DoubleValid); ok {
		for i, valid := range td.DoubleValid {
			if valid {
				assert.InDelta(t, td.DoublePoints[i].Content(), dbl.Values[i], 0, "row %d", i)
			} else {
				assert.True(t, math.IsNaN(dbl.Values[i]), "row %d", i)
			}
		}
	}
	if i64, ok := fixtureColumn[*QueryColumnInt64](t, rs, rsInt64Index, td.Int64Valid); ok {
		for i, valid := range td.Int64Valid {
			if valid {
				assert.Equal(t, td.Int64Points[i].Content(), i64.Values[i], "row %d", i)
			} else {
				assert.Equal(t, int64(math.MinInt64), i64.Values[i], "row %d", i)
			}
		}
	}
	if str, ok := fixtureColumn[*QueryColumnString](t, rs, rsStringIndex, td.StringValid); ok {
		for i, valid := range td.StringValid {
			if valid {
				assert.Equal(t, td.StringPoints[i].Content(), str.Value(i), "row %d", i)
			} else {
				assert.Equal(t, "", str.Value(i), "row %d", i)
			}
		}
	}
	if ts, ok := fixtureColumn[*QueryColumnTimestamp](t, rs, rsTsIndex, td.TimestampValid); ok {
		for i, valid := range td.TimestampValid {
			if valid {
				assert.Equal(t, td.TimestampPoints[i].Content().UnixNano(), ts.Nanos[i], "row %d", i)
				assert.True(t, td.TimestampPoints[i].Content().Equal(ts.Time(i)), "row %d", i)
			} else {
				assert.Equal(t, int64(math.MinInt64), ts.Nanos[i], "row %d", i)
			}
		}
	}
	if sym, ok := fixtureColumn[*QueryColumnString](t, rs, rsSymbolIndex, td.SymbolValid); ok {
		for i, valid := range td.SymbolValid {
			if valid {
				assert.Equal(t, td.SymbolPoints[i].Content(), sym.Value(i), "row %d", i)
			} else {
				assert.Equal(t, "", sym.Value(i), "row %d", i)
			}
		}
	}
}

// ---------------------------------------------------------------------
// Column accessors on hand-filled columns
// ---------------------------------------------------------------------

func TestQueryResultSetFixedColumnsExposeBuffers(t *testing.T) {
	i64 := newQueryColumnInt64("i", 3)
	i64.Values[1] = 42
	i64.valid.set(1)
	assert.Equal(t, "i", i64.Name())
	assert.Equal(t, 3, i64.Len())
	assert.Equal(t, 2, i64.Valid().NullCount())
	assert.True(t, i64.Valid().IsValid(1))

	dbl := newQueryColumnDouble("d", 2)
	dbl.Values[0] = 1.5
	assert.Equal(t, "d", dbl.Name())
	assert.Equal(t, 2, dbl.Len())
	assert.True(t, dbl.Valid().AllNull())

	ts := newQueryColumnTimestamp("t", 1)
	ts.Nanos[0] = time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC).UnixNano()
	assert.Equal(t, "t", ts.Name())
	assert.Equal(t, 1, ts.Len())
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC), ts.Time(0))
	assert.Equal(t, time.UTC, ts.Time(0).Location())

	null := newQueryColumnNull("n", 5)
	assert.Equal(t, "n", null.Name())
	assert.Equal(t, 5, null.Len())
	assert.True(t, null.Valid().AllNull())
}

func TestQueryResultSetVarColumnsExposeBuffers(t *testing.T) {
	cells := [][]byte{[]byte("ab"), nil, []byte("cde"), nil}
	valid := []bool{true, true, true, false}
	str := newQueryColumnString("s", len(cells), 5)
	fillVarBytes(&str.varBytes, cells, valid)

	assert.Equal(t, "s", str.Name())
	assert.Equal(t, 4, str.Len())
	assert.Equal(t, []string{"ab", "", "cde", ""}, []string{str.Value(0), str.Value(1), str.Value(2), str.Value(3)},
		"empty cell and null cell at the end of the buffer both read as empty")
	assert.Equal(t, []int32{0, 2, 2, 5, 5}, str.Offsets())
	assert.Equal(t, []byte("abcde"), str.Bytes())
	assert.Equal(t, 1, str.Valid().NullCount())

	blob := newQueryColumnBlob("b", 2, 4)
	fillVarBytes(&blob.varBytes, [][]byte{[]byte("ab"), []byte("cd")}, []bool{true, true})
	first := blob.Value(0)
	require.Equal(t, []byte("ab"), first)
	require.Equal(t, 2, cap(first))

	// An append must reallocate rather than write into the next cell.
	_ = append(first, 'x') //nolint:staticcheck // the side effect on the buffer is what is under test
	assert.Equal(t, []byte("cd"), blob.Value(1))
	assert.Equal(t, []byte("abcd"), blob.Bytes())
}

func TestQueryResultSetLookup(t *testing.T) {
	first := newQueryColumnInt64("a", 1)
	second := newQueryColumnDouble("a", 1)
	other := newQueryColumnNull("b", 1)
	rs := newQueryResultSet([]QueryColumn{first, second, other}, 1, 7)

	assert.Equal(t, 1, rs.RowCount())
	assert.Equal(t, int64(7), rs.ScannedPoints())
	assert.Len(t, rs.Columns(), 3)

	col, ok := rs.Column("a")
	require.True(t, ok)
	assert.Same(t, first, col, "duplicate names resolve to the first column")
	col, ok = rs.Column("b")
	require.True(t, ok)
	assert.Same(t, other, col)
	_, ok = rs.Column("missing")
	assert.False(t, ok)

	cases := []struct {
		name    string
		lookup  func() (QueryColumn, error)
		wantErr ErrorType
		wantMsg []string
	}{
		{"found", func() (QueryColumn, error) { return ColumnOf[*QueryColumnInt64](rs, "a") }, Success, nil},
		{"unknown name", func() (QueryColumn, error) { return ColumnOf[*QueryColumnInt64](rs, "missing") }, ErrElementNotFound, []string{"missing"}},
		{"wrong type", func() (QueryColumn, error) { return ColumnOf[*QueryColumnString](rs, "a") }, ErrIncompatibleType, []string{"*qdb.QueryColumnString", "*qdb.QueryColumnInt64"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.lookup()
			if c.wantErr == Success {
				require.NoError(t, err)
				assert.Same(t, first, got)

				return
			}
			require.Error(t, err)
			assert.True(t, errors.Is(err, c.wantErr), err.Error())
			for _, m := range c.wantMsg {
				assert.Contains(t, err.Error(), m)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Layout, fixtures and pure helpers
// ---------------------------------------------------------------------

// TestQueryResultSetCellLayout pins the offsets the payload loads depend on;
// the sizes are pinned at compile time in query_result_convert.go.
func TestQueryResultSetCellLayout(t *testing.T) {
	var cell QueryPoint
	var ts Timespec
	assert.Equal(t, uintptr(24), unsafe.Sizeof(cell))
	assert.Equal(t, uintptr(0), unsafe.Offsetof(cell._type))
	assert.Equal(t, uintptr(8), unsafe.Offsetof(cell.payload))
	assert.Equal(t, uintptr(16), unsafe.Sizeof(cell.payload))
	assert.Equal(t, uintptr(0), unsafe.Offsetof(ts.tv_sec))
	assert.Equal(t, uintptr(8), unsafe.Offsetof(ts.tv_nsec))
}

// TestPointRowsFixtureReadsBackThroughAccessors checks the hand-built rows
// through the legacy per-cell accessors, so the conversion tests rest on a
// fixture whose layout is known good.
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

func TestMergeValueTypeNullIsIdentity(t *testing.T) {
	all := []TsValueType{TsValueNull, TsValueInt64, TsValueDouble, TsValueTimestamp, TsValueString, TsValueBlob}
	for _, k := range all {
		got, ok := mergeValueType(TsValueNull, k)
		require.True(t, ok)
		assert.Equal(t, k, got)

		got, ok = mergeValueType(k, TsValueNull)
		require.True(t, ok)
		assert.Equal(t, k, got)

		got, ok = mergeValueType(k, k)
		require.True(t, ok)
		assert.Equal(t, k, got)
	}

	for _, a := range all[1:] {
		for _, b := range all[1:] {
			if a == b {
				continue
			}
			_, ok := mergeValueType(a, b)
			assert.False(t, ok, "%v + %v", a, b)
		}
	}
}

// ---------------------------------------------------------------------
// Conversion of hand-built rows
// ---------------------------------------------------------------------

type resultSetFromRowsCase struct {
	name    string
	names   []string
	rows    [][]testCellFunc
	wantErr ErrorType
	wantMsg []string
	check   func(t *testing.T, rs *QueryResultSet)
}

func resultSetFromRowsCases() []resultSetFromRowsCase {
	cases := []resultSetFromRowsCase{
		{
			name:  "fixed columns with nulls and count folded into int64",
			names: []string{"i", "d", "t", "c", "n"},
			rows: [][]testCellFunc{
				{testCellInt64(7), testCellDouble(1.5), testCellTimestamp(10, 20), testCellCount(3), testCellNone()},
				{testCellNone(), testCellNone(), testCellNone(), testCellNone(), testCellNone()},
				{testCellInt64(-7), testCellDouble(-1.5), testCellTimestamp(-10, 20), testCellInt64(4), testCellNone()},
			},
			check: checkFixedColumns,
		},
		{
			name:  "var columns with nulls, empty cells and a stale payload under a none tag",
			names: []string{"s", "b"},
			rows: [][]testCellFunc{
				{testCellString("ab"), testCellBlob([]byte{1})},
				{testCellNone(), testCellNone()},
				{testCellString(""), testCellBlob(nil)},
				{testCellSeq(testCellString("stale"), testCellNone()), testCellBlob([]byte{2, 3})},
				{testCellString("cde"), testCellNone()},
			},
			check: checkVarColumns,
		},
		{
			name:  "no rows yields null columns of length zero",
			names: []string{"a", "b"},
			check: func(t *testing.T, rs *QueryResultSet) {
				t.Helper()
				assert.Equal(t, 0, rs.RowCount())
				require.Len(t, rs.Columns(), 2)
				for _, c := range rs.Columns() {
					_, ok := c.(*QueryColumnNull)
					assert.True(t, ok, "%T", c)
					assert.Equal(t, 0, c.Len())
				}
			},
		},
		{
			name:    "two typed kinds in one column",
			names:   []string{"mixed"},
			rows:    [][]testCellFunc{{testCellInt64(1)}, {testCellDouble(1)}},
			wantErr: ErrIncompatibleType,
			wantMsg: []string{"mixed", "int64", "double"},
		},
		{
			name:    "timestamp outside the int64 nanosecond range",
			names:   []string{"when"},
			rows:    [][]testCellFunc{{testCellTimestamp(0, 0)}, {testCellTimestamp(maxTimespecSec+1, 0)}},
			wantErr: ErrOutOfBounds,
			wantMsg: []string{"when", "row=1"},
		},
		{
			name:    "blob column past int32 offsets",
			names:   []string{"big"},
			rows:    [][]testCellFunc{{testCellBlob([]byte{1})}, {testCellBlobUnbacked(math.MaxInt32)}},
			wantErr: ErrOutOfBounds,
			wantMsg: []string{"big"},
		},
	}
	for _, tag := range testArrayTags() {
		cases = append(cases, resultSetFromRowsCase{
			name:    fmt.Sprintf("array tag %d", tag),
			names:   []string{"arr"},
			rows:    [][]testCellFunc{{testCellTagged(tag)}},
			wantErr: ErrNotImplemented,
			wantMsg: []string{"arr"},
		})
	}

	return cases
}

func checkFixedColumns(t *testing.T, rs *QueryResultSet) {
	t.Helper()
	require.Equal(t, 3, rs.RowCount())

	i64, err := ColumnOf[*QueryColumnInt64](rs, "i")
	require.NoError(t, err)
	assert.Equal(t, []int64{7, math.MinInt64, -7}, i64.Values)
	assert.Equal(t, []bool{true, false, true}, validBits(i64.Valid()))

	dbl, err := ColumnOf[*QueryColumnDouble](rs, "d")
	require.NoError(t, err)
	assert.InDelta(t, 1.5, dbl.Values[0], 0)
	assert.True(t, math.IsNaN(dbl.Values[1]))
	assert.InDelta(t, -1.5, dbl.Values[2], 0)
	assert.Equal(t, []bool{true, false, true}, validBits(dbl.Valid()))

	ts, err := ColumnOf[*QueryColumnTimestamp](rs, "t")
	require.NoError(t, err)
	assert.Equal(t, []int64{10_000_000_020, math.MinInt64, -9_999_999_980}, ts.Nanos)
	assert.Equal(t, time.Unix(10, 20).UTC(), ts.Time(0))
	assert.Equal(t, []bool{true, false, true}, validBits(ts.Valid()))

	cnt, err := ColumnOf[*QueryColumnInt64](rs, "c")
	require.NoError(t, err)
	assert.Equal(t, []int64{3, math.MinInt64, 4}, cnt.Values)

	null, err := ColumnOf[*QueryColumnNull](rs, "n")
	require.NoError(t, err)
	assert.Equal(t, 3, null.Len())
	assert.True(t, null.Valid().AllNull())
}

func checkVarColumns(t *testing.T, rs *QueryResultSet) {
	t.Helper()
	require.Equal(t, 5, rs.RowCount())

	str, err := ColumnOf[*QueryColumnString](rs, "s")
	require.NoError(t, err)
	assert.Equal(t, []string{"ab", "", "", "", "cde"}, []string{str.Value(0), str.Value(1), str.Value(2), str.Value(3), str.Value(4)})
	assert.Equal(t, []int32{0, 2, 2, 2, 2, 5}, str.Offsets(), "stale payload under a none tag is not copied")
	assert.Equal(t, []byte("abcde"), str.Bytes())
	assert.Equal(t, []bool{true, false, true, false, true}, validBits(str.Valid()))

	blob, err := ColumnOf[*QueryColumnBlob](rs, "b")
	require.NoError(t, err)
	assert.Equal(t, []byte{1}, blob.Value(0))
	assert.Empty(t, blob.Value(1))
	assert.Empty(t, blob.Value(2))
	assert.Equal(t, []byte{2, 3}, blob.Value(3))
	assert.Empty(t, blob.Value(4))
	assert.Equal(t, []int32{0, 1, 1, 1, 3, 3}, blob.Offsets())
	assert.Equal(t, []bool{true, false, true, true, false}, validBits(blob.Valid()))
}

func TestResultSetFromRows(t *testing.T) {
	handle := newTestHandle(t)
	for _, c := range resultSetFromRowsCases() {
		t.Run(c.name, func(t *testing.T) {
			rows := newTestPointRows(t, handle, c.rows)
			rs, err := resultSetFromRows(c.names, rows, 7)
			if c.wantErr != Success {
				require.Error(t, err)
				assert.True(t, errors.Is(err, c.wantErr), err.Error())
				for _, m := range c.wantMsg {
					assert.Contains(t, err.Error(), m)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, int64(7), rs.ScannedPoints())
			for i, col := range rs.Columns() {
				assert.Equal(t, c.names[i], col.Name())
			}
			c.check(t, rs)
		})
	}
}

// ---------------------------------------------------------------------
// Live cluster
// ---------------------------------------------------------------------

// TestFetchMatchesFixture runs the fixture at several sizes and sparsities
// through both entry points. The result set built from Execute is checked only
// after the result is closed and the heap scrubbed, which proves it aliases
// no C memory.
func TestFetchMatchesFixture(t *testing.T) {
	handle := newTestHandle(t)
	cases := []struct{ count, sparsity int }{{1, 100}, {8, 100}, {64, 50}, {65, 50}, {4, 0}}
	for _, c := range cases {
		t.Run(fmt.Sprintf("count=%d sparsity=%d", c.count, c.sparsity), func(t *testing.T) {
			td := newTestTimeseriesAllColumnsSparse(t, handle, int64(c.count), c.sparsity)

			fetched, err := handle.Query(selectFixture(td)).Fetch()
			require.NoError(t, err)
			require.NotNil(t, fetched)
			assert.Positive(t, fetched.ScannedPoints())
			requireResultSetMatchesFixture(t, fetched, td)

			result, err := handle.Query(selectFixture(td)).Execute()
			require.NoError(t, err)
			names := result.ColumnsNames()
			converted, err := result.ToResultSet()
			require.NoError(t, err)
			result.Close()
			runtime.GC()
			debug.FreeOSMemory()

			for i, col := range converted.Columns() {
				assert.Equal(t, names[i], col.Name())
			}
			requireResultSetMatchesFixture(t, converted, td)
		})
	}
}

func TestFetchEdgeCases(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 16, 50)

	t.Run("nil result converts to an empty result set", func(t *testing.T) {
		var nilResult *QueryResult
		rs, err := nilResult.ToResultSet()
		require.NoError(t, err)
		assert.Equal(t, 0, rs.RowCount())
		assert.Empty(t, rs.Columns())
	})

	t.Run("closed result converts to an empty result set", func(t *testing.T) {
		result, err := handle.Query(selectFixture(td)).Execute()
		require.NoError(t, err)
		result.Close()

		rs, err := result.ToResultSet()
		require.NoError(t, err)
		assert.Equal(t, 0, rs.RowCount())
		assert.Empty(t, rs.Columns())
	})

	t.Run("empty range keeps the column set with no rows", func(t *testing.T) {
		result, err := handle.Query(fmt.Sprintf("select * from %s in range(1971, +10d)", td.Alias)).Execute()
		require.NoError(t, err)
		defer result.Close()

		rs, err := result.ToResultSet()
		require.NoError(t, err)
		assert.Equal(t, 0, rs.RowCount())
		assert.Len(t, rs.Columns(), int(result.ColumnsCount()))
		for _, c := range rs.Columns() {
			assert.Equal(t, 0, c.Len())
		}
	})

	t.Run("count aggregate is an int64 column", func(t *testing.T) {
		result, err := handle.Query(selectFixture(td)).Execute()
		require.NoError(t, err)
		names := result.ColumnsNames()
		result.Close()

		rs, err := handle.Query(fmt.Sprintf("select count(%s) from %s in range(1970, +10d)", names[rsInt64Index], td.Alias)).Fetch()
		require.NoError(t, err)
		require.Equal(t, 1, rs.RowCount())
		require.Len(t, rs.Columns(), 1)

		cnt, err := ColumnOf[*QueryColumnInt64](rs, rs.Columns()[0].Name())
		require.NoError(t, err)
		valid := 0
		for _, ok := range td.Int64Valid {
			if ok {
				valid++
			}
		}
		assert.Equal(t, []int64{int64(valid)}, cnt.Values)
		assert.True(t, cnt.Valid().AllValid())
	})

	t.Run("ddl yields a nil result set", func(t *testing.T) {
		alias := generateAlias(16)
		rs, err := handle.Query(fmt.Sprintf("create table %s ($timestamp TIMESTAMP, id INT64)", alias)).Fetch()
		require.NoError(t, err)
		assert.Nil(t, rs)

		rs, err = handle.Query(fmt.Sprintf("drop table %s", alias)).Fetch()
		require.NoError(t, err)
		assert.Nil(t, rs)
	})

	t.Run("invalid query is an error", func(t *testing.T) {
		rs, err := handle.Query("select").Fetch()
		require.Error(t, err)
		assert.Nil(t, rs)
	})
}
