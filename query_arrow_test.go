package qdb

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireArrowColumnMatches asserts that an imported Arrow array carries the
// same name, validity and values as the QueryResultSet column produced by
// Fetch for the same query. The row-major path is the reference: it is
// exercised by the fixture tests in query_result_test.go.
func requireArrowColumnMatches(t *testing.T, field arrow.Field, arr arrow.Array, col QueryColumn) {
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
		// path has the table type when the column comes from a table.
		assert.Equal(t, c.Len(), arr.Len())
		assert.Equal(t, arr.Len(), arr.NullN(), "every slot is null")
	default:
		t.Fatalf("unexpected column type %T", col)
	}
}

// requireArrowValuesMatch compares the Arrow array against a masked value
// slice slot by slot: validity first, then the value for valid slots only,
// since null slots hold a sentinel on the Go side and garbage on the Arrow
// side.
func requireArrowValuesMatch[A arrow.Array, V any](t *testing.T, arr arrow.Array, valid Mask, values []V, get func(A, int) V) {
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

// assertArrowMaxWidth asserts the C side attached its max_width metadata to a
// variable-width field.
func assertArrowMaxWidth(t *testing.T, field arrow.Field) {
	t.Helper()

	_, ok := field.Metadata.GetValue("max_width")
	assert.True(t, ok, "field %q carries max_width metadata: %v", field.Name, field.Metadata)
}

// ---------------------------------------------------------------------
// qdb_query_arrow binding
// ---------------------------------------------------------------------

func TestExecuteArrowBinding(t *testing.T) {
	handle := newTestHandle(t)
	td := newTestTimeseriesAllColumnsSparse(t, handle, 16, 50)

	t.Run("select yields one column per fixture column", func(t *testing.T) {
		r, err := handle.Query(selectFixture(td)).executeArrow()
		require.NoError(t, err)
		require.NotNil(t, r)
		defer r.Close()

		cols := r.columns()
		require.Len(t, cols, 8)
		// The schema name is the query's column name; the fixture selects *,
		// so the C side fills in the table's column names.
		assert.Equal(t, "$timestamp", arrowColumnName(&cols[rsTimestampIndex]))
		assert.Equal(t, "$table", arrowColumnName(&cols[rsTableIndex]))
		for i := range cols {
			assert.False(t, arrowColumnMoved(&cols[i]), "column %d", i)
		}
	})

	t.Run("ddl yields a nil result", func(t *testing.T) {
		alias := generateAlias(16)
		r, err := handle.Query(fmt.Sprintf("create table %s ($timestamp TIMESTAMP, id INT64)", alias)).executeArrow()
		require.NoError(t, err)
		assert.Nil(t, r)

		r, err = handle.Query(fmt.Sprintf("drop table %s", alias)).executeArrow()
		require.NoError(t, err)
		assert.Nil(t, r)
	})

	t.Run("invalid query yields an error and no result", func(t *testing.T) {
		r, err := handle.Query("SELECT FROM").executeArrow()
		require.ErrorIs(t, err, ErrInvalidQuery)
		assert.Nil(t, r)
	})

	t.Run("close is nil-safe and idempotent", func(t *testing.T) {
		var nilResult *queryArrowResult
		nilResult.Close()
		assert.Nil(t, nilResult.columns())

		r, err := handle.Query(selectFixture(td)).executeArrow()
		require.NoError(t, err)
		r.Close()
		r.Close()
		assert.Nil(t, r.columns())
	})
}

// ---------------------------------------------------------------------
// Column import
// ---------------------------------------------------------------------

func TestImportArrowColumn(t *testing.T) {
	handle := newTestHandle(t)
	cases := []struct{ count, sparsity int }{{1, 100}, {8, 100}, {64, 50}, {4, 0}}
	for _, c := range cases {
		t.Run(fmt.Sprintf("count=%d sparsity=%d", c.count, c.sparsity), func(t *testing.T) {
			td := newTestTimeseriesAllColumnsSparse(t, handle, int64(c.count), c.sparsity)
			rs, err := handle.Query(selectFixture(td)).Fetch()
			require.NoError(t, err)

			r, err := handle.Query(selectFixture(td)).executeArrow()
			require.NoError(t, err)
			defer r.Close()

			cols := r.columns()
			require.Len(t, cols, len(rs.Columns()))
			arrays := make([]arrow.Array, len(cols))
			defer releaseArrowArrays(arrays)
			for j := range cols {
				field, arr, err := importArrowColumn(&cols[j])
				require.NoError(t, err)
				arrays[j] = arr
				assert.True(t, arrowColumnMoved(&cols[j]), "column %d is marked released after import", j)
				requireArrowColumnMatches(t, field, arr, rs.Columns()[j])
			}
		})
	}
}

func TestReleaseArrowArrays(t *testing.T) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, 2, 3}, nil)
	arr := b.NewArray()
	b.Release()
	arr.Retain()
	releaseArrowArrays([]arrow.Array{arr, nil})
	assert.Equal(t, 3, arr.Len(), "still alive after one release of two references")
	arr.Release()
}

// ---------------------------------------------------------------------
// Record batch assembly
// ---------------------------------------------------------------------

// newTestInt64Array builds a Go-owned Int64 array; the caller releases it.
func newTestInt64Array(vs []int64) arrow.Array { //nolint:ireturn // Justified: arrow.Array is arrow-go's array interface
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vs, nil)

	return b.NewArray()
}

func TestArrowRecordFromColumns(t *testing.T) {
	fields := []arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}

	t.Run("equal lengths give a batch over the same arrays", func(t *testing.T) {
		arrays := []arrow.Array{newTestInt64Array([]int64{1, 2, 3}), newTestInt64Array([]int64{4, 5, 6})}
		defer releaseArrowArrays(arrays)

		rec, err := arrowRecordFromColumns(fields, arrays)
		require.NoError(t, err)
		defer rec.Release()

		assert.Equal(t, int64(3), rec.NumRows())
		assert.Equal(t, int64(2), rec.NumCols())
		assert.Equal(t, []string{"a", "b"}, []string{rec.Schema().Field(0).Name, rec.Schema().Field(1).Name})
		assert.Equal(t, int64(6), rec.Column(1).(*array.Int64).Value(2))
	})

	t.Run("zero rows give an empty batch with the full schema", func(t *testing.T) {
		arrays := []arrow.Array{newTestInt64Array(nil), newTestInt64Array(nil)}
		defer releaseArrowArrays(arrays)

		rec, err := arrowRecordFromColumns(fields, arrays)
		require.NoError(t, err)
		defer rec.Release()

		assert.Equal(t, int64(0), rec.NumRows())
		assert.Equal(t, 2, rec.Schema().NumFields())
	})

	t.Run("length mismatch is an error, not a panic", func(t *testing.T) {
		arrays := []arrow.Array{newTestInt64Array([]int64{1, 2, 3}), newTestInt64Array([]int64{4})}
		defer releaseArrowArrays(arrays)

		rec, err := arrowRecordFromColumns(fields, arrays)
		require.ErrorIs(t, err, ErrInvalidArgument)
		assert.Nil(t, rec)
	})
}
