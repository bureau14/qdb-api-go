package qdb

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// requireArrowRecordMatches asserts the batch carries the same columns as the
// result set Fetch produced for the same query. The row-major path is the
// reference; the fixture tests in query_result_test.go establish it.
func requireArrowRecordMatches(t testHelper, rec arrow.RecordBatch, rs *QueryResultSet) {
	t.Helper()

	require.NotNil(t, rec)
	require.Equal(t, int64(rs.RowCount()), rec.NumRows())
	require.Equal(t, len(rs.Columns()), rec.Schema().NumFields())
	for j, col := range rs.Columns() {
		requireArrowColumnMatches(t, rec.Schema().Field(j), rec.Column(j), col)
	}
}

// requireArrowColumnMatches asserts one imported column against its
// QueryResultSet counterpart: name, nullability, type, validity and values.
func requireArrowColumnMatches(t testHelper, field arrow.Field, arr arrow.Array, col QueryColumn) {
	t.Helper()

	assert.Equal(t, col.Name(), field.Name)
	assert.True(t, field.Nullable, "every field is nullable")
	switch c := col.(type) {
	case *QueryColumnInt64:
		requireArrowValuesMatch(t, arr, c.Valid(), c.Values, func(a *array.Int64, i int) int64 { return a.Value(i) })
	case *QueryColumnDouble:
		requireArrowValuesMatch(t, arr, c.Valid(), c.Values, func(a *array.Float64, i int) float64 { return a.Value(i) })
	case *QueryColumnTimestamp:
		assert.Equal(t, arrow.Nanosecond, field.Type.(*arrow.TimestampType).Unit)
		assert.Empty(t, field.Type.(*arrow.TimestampType).TimeZone, "timestamps pass through naive")
		requireArrowValuesMatch(t, arr, c.Valid(), c.Values, func(a *array.Timestamp, i int) int64 { return int64(a.Value(i)) })
	case *QueryColumnString:
		assertArrowMaxWidth(t, field)
		requireArrowValuesMatch(t, arr, c.Valid(), c.Values, func(a *array.String, i int) string { return a.Value(i) })
	case *QueryColumnBlob:
		assertArrowMaxWidth(t, field)
		requireArrowValuesMatch(t, arr, c.Valid(), c.Values, func(a *array.Binary, i int) []byte { return a.Value(i) })
	case *QueryColumnNull:
		// The row-major path has no type for an all-null column; the Arrow
		// path keeps the table type and marks every slot null.
		assert.NotEqual(t, arrow.NULL, field.Type.ID(), "all-null column keeps the table type")
		assert.Equal(t, c.Len(), arr.Len())
		assert.Equal(t, arr.Len(), arr.NullN(), "every slot is null")
	default:
		require.Failf(t, "unexpected column type", "%T", col)
	}
}

// requireArrowValuesMatch compares validity slot by slot and values on valid
// slots only: null slots hold a sentinel on the Go side and are undefined on
// the Arrow side.
func requireArrowValuesMatch[A arrow.Array, V any](t testHelper, arr arrow.Array, valid Mask, values []V, get func(A, int) V) {
	t.Helper()

	typed, ok := arr.(A)
	require.True(t, ok, "array type %T", arr)
	require.Equal(t, len(values), typed.Len())
	for i, v := range values {
		require.Equal(t, valid.IsValid(i), typed.IsValid(i), "validity of row %d", i)
		if valid.IsValid(i) {
			assert.Equal(t, v, get(typed, i), "row %d", i)
		}
	}
}

// assertArrowMaxWidth asserts the C side attached max_width metadata to a
// variable-width field.
func assertArrowMaxWidth(t testHelper, field arrow.Field) {
	t.Helper()

	_, ok := field.Metadata.GetValue("max_width")
	assert.True(t, ok, "field %q carries max_width metadata: %v", field.Name, field.Metadata)
}

// newTestInt64Array builds a Go-owned Int64 array; the caller releases it.
func newTestInt64Array(vs []int64) arrow.Array { //nolint:ireturn // Justified: arrow.Array is arrow-go's array interface
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vs, nil)

	return b.NewArray()
}

// TestFetchArrowMatchesFetch loads the all-columns fixture at a generated
// size and sparsity and checks FetchArrow against Fetch column by column,
// after a heap scrub. This drives execution, import and assembly end to end
// over every column type, with nulls, all-null columns and empty strings
// whenever the draw produces them.
func TestFetchArrowMatchesFetch(t *testing.T) {
	handle := newTestHandle(t)
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.Int64Range(1, 128).Draw(rt, "count")
		sparsity := rapid.IntRange(0, 100).Draw(rt, "sparsity")
		td := newTestTimeseriesAllColumnsSparse(rt, handle, count, sparsity)

		rs, err := handle.Query(selectFixture(td)).Fetch()
		require.NoError(rt, err)
		rec, err := handle.Query(selectFixture(td)).FetchArrow()
		require.NoError(rt, err)
		defer rec.Release()
		runtime.GC()
		debug.FreeOSMemory()

		requireArrowRecordMatches(rt, rec, rs)
	})
}

func TestFetchArrowEdgeCases(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 16, 50)

	t.Run("ddl yields a nil batch", func(t *testing.T) {
		alias := generateAlias(16)
		rec, err := handle.Query(fmt.Sprintf("create table %s ($timestamp TIMESTAMP, id INT64)", alias)).FetchArrow()
		require.NoError(t, err)
		assert.Nil(t, rec)

		rec, err = handle.Query(fmt.Sprintf("drop table %s", alias)).FetchArrow()
		require.NoError(t, err)
		assert.Nil(t, rec)
	})

	t.Run("zero rows yield a zero-row batch with every column", func(t *testing.T) {
		rec, err := handle.Query(fmt.Sprintf("select * from %s in range(2000, +1d)", td.Alias)).FetchArrow()
		require.NoError(t, err)
		require.NotNil(t, rec)
		defer rec.Release()

		assert.Equal(t, int64(0), rec.NumRows())
		assert.Equal(t, 8, rec.Schema().NumFields())
	})

	t.Run("count aggregate is an int64 column", func(t *testing.T) {
		rs, err := handle.Query(selectFixture(td)).Fetch()
		require.NoError(t, err)
		name := rs.Columns()[rsInt64Index].Name()

		rec, err := handle.Query(fmt.Sprintf("select count(%s) from %s in range(1970, +10d)", name, td.Alias)).FetchArrow()
		require.NoError(t, err)
		defer rec.Release()

		require.Equal(t, int64(1), rec.NumRows())
		cnt, ok := rec.Column(0).(*array.Int64)
		require.True(t, ok, "column type %T", rec.Column(0))
		i64, err := ColumnOf[*QueryColumnInt64](rs, name)
		require.NoError(t, err)
		assert.Equal(t, int64(i64.Len()-i64.Valid().NullCount()), cnt.Value(0))
	})

	t.Run("missing table yields an error and no batch", func(t *testing.T) {
		rec, err := handle.Query(fmt.Sprintf("select * from %s in range(1970, +1d)", generateAlias(16))).FetchArrow()
		require.ErrorIs(t, err, ErrAliasNotFound)
		assert.Nil(t, rec)
	})

	t.Run("invalid query yields an error and no batch", func(t *testing.T) {
		rec, err := handle.Query("SELECT FROM").FetchArrow()
		require.ErrorIs(t, err, ErrInvalidQuery)
		assert.Nil(t, rec)
	})

	t.Run("column length mismatch is an error, not a panic", func(t *testing.T) {
		fields := []arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		arrays := []arrow.Array{newTestInt64Array([]int64{1, 2, 3}), newTestInt64Array([]int64{4})}
		defer releaseArrowArrays(arrays)

		rec, err := arrowRecordFromColumns(fields, arrays)
		require.ErrorIs(t, err, ErrInvalidArgument)
		assert.Nil(t, rec)
	})
}

// TestFetchArrowOutlivesHandle closes the handle the batch came from, scrubs
// the heap, and reads the batch afterwards: the buffers belong to the batch,
// not to the handle.
func TestFetchArrowOutlivesHandle(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 32, 50)
	rs, err := handle.Query(selectFixture(td)).Fetch()
	require.NoError(t, err)

	other, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(t, err)
	rec, err := other.Query(selectFixture(td)).FetchArrow()
	require.NoError(t, err)
	defer rec.Release()
	require.NoError(t, other.Close())
	runtime.GC()
	debug.FreeOSMemory()

	requireArrowRecordMatches(t, rec, rs)
}
