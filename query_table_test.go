package qdb

import (
	"testing"
	"time"

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
