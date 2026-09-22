package qdb

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// drainArrowStreams pulls every stream through fetchArrowStream and drain,
// returning the retained records.
func drainArrowStreams(t testHelper, r *Reader) []arrow.RecordBatch {
	t.Helper()

	var recs []arrow.RecordBatch
	for {
		stream, err := r.fetchArrowStream()
		if errors.Is(err, ErrIteratorEnd) {
			return recs
		}
		require.NoError(t, err)

		more := stream.drain(func(rec arrow.RecordBatch, err error) bool {
			require.NoError(t, err)
			recs = append(recs, rec)

			return true
		})
		require.True(t, more)
	}
}

func releaseRecords(recs []arrow.RecordBatch) {
	for _, rec := range recs {
		rec.Release()
	}
}

func TestReaderArrowStreamEmptyTableEndsImmediately(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	stream, err := reader.fetchArrowStream()
	assert.Nil(t, stream.reader)
	assert.ErrorIs(t, err, ErrIteratorEnd)
}

func TestReaderArrowStreamRecordsSurviveReaderRelease(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)

		WithGCAndHandle(rt, handle, "TestReaderArrowStreamRecordsSurviveReaderRelease", func() {
			tables := genPopulatedTables(rt, handle)
			pushWriterTables(t, handle, tables)

			reader, err := NewReader(handle, NewReaderDefaultOptions(writerTableNames(tables)))
			require.NoError(rt, err)

			recs := drainArrowStreams(rt, &reader)
			defer releaseRecords(recs)

			reader.Close()

			total := int64(0)
			for _, rec := range recs {
				total += rec.NumRows()
				assert.Equal(rt, "$table", rec.Schema().Field(0).Name)
				assert.Equal(rt, "$timestamp", rec.Schema().Field(1).Name)
			}

			expected := int64(0)
			for _, wt := range tables {
				expected += int64(wt.RowCount())
			}
			assert.Equal(rt, expected, total)
		})
	})
}

// collectArrow drains an Arrow sequence, failing on any error step. The
// caller releases the records.
func collectArrow(t testHelper, r *Reader) []arrow.RecordBatch {
	t.Helper()

	var recs []arrow.RecordBatch
	for rec, err := range r.Arrow() {
		require.NoError(t, err)
		recs = append(recs, rec)
	}

	return recs
}

func totalRows(recs []arrow.RecordBatch) int64 {
	n := int64(0)
	for _, rec := range recs {
		n += rec.NumRows()
	}

	return n
}

func writerTablesRowCount(tables []WriterTable) int64 {
	n := int64(0)
	for _, wt := range tables {
		n += int64(wt.RowCount())
	}

	return n
}

// assertLegacyArrowSchema checks the layout the reader uses when no columns
// are requested: "$table", "$timestamp", then the table's columns in order.
func assertLegacyArrowSchema(t testHelper, schema *arrow.Schema, cols []WriterColumn) {
	t.Helper()

	require.Equal(t, 2+len(cols), schema.NumFields())

	table := schema.Field(0)
	assert.Equal(t, "$table", table.Name)
	assert.Equal(t, arrow.STRING, table.Type.ID())
	assert.False(t, table.Nullable)

	ts := schema.Field(1)
	assert.Equal(t, "$timestamp", ts.Name)
	tsType, ok := ts.Type.(*arrow.TimestampType)
	require.True(t, ok, "field type %v", ts.Type)
	assert.Equal(t, arrow.Nanosecond, tsType.Unit)
	assert.Empty(t, tsType.TimeZone)
	assert.False(t, ts.Nullable)

	for j, col := range cols {
		field := schema.Field(2 + j)
		assert.Equal(t, col.ColumnName, field.Name)
		assert.True(t, field.Nullable, "data column %q is nullable", field.Name)
		assert.Equal(t, arrowTypeIDOf(col.ColumnType), field.Type.ID(), "column %q", field.Name)
	}
}

// arrowTypeIDOf maps a table column type to the Arrow type the reader emits.
func arrowTypeIDOf(ctype TsColumnType) arrow.Type {
	switch ctype {
	case TsColumnInt64:
		return arrow.INT64
	case TsColumnDouble:
		return arrow.FLOAT64
	case TsColumnTimestamp:
		return arrow.TIMESTAMP
	case TsColumnBlob:
		return arrow.BINARY
	case TsColumnString, TsColumnSymbol:
		return arrow.STRING
	case TsColumnUninitialized:
		return arrow.NULL
	}

	return arrow.NULL
}

// assertArrowRecordsEqualWriterTables checks every row of every batch
// against the table it came from, matching rows on "$timestamp".
func assertArrowRecordsEqualWriterTables(t testHelper, tables []WriterTable, recs []arrow.RecordBatch) {
	t.Helper()

	byName := make(map[string]WriterTable, len(tables))
	for _, wt := range tables {
		byName[wt.GetName()] = wt
	}

	for _, rec := range recs {
		assertLegacyArrowSchema(t, rec.Schema(), writerTableColumns(tables[0]))
		names, ok := rec.Column(0).(*array.String)
		require.True(t, ok)
		stamps, ok := rec.Column(1).(*array.Timestamp)
		require.True(t, ok)

		for i := range int(rec.NumRows()) {
			wt, found := byName[names.Value(i)]
			require.True(t, found, "unknown table %q", names.Value(i))
			k := rowOffsetAt(wt, time.Unix(0, int64(stamps.Value(i))).UTC())
			require.GreaterOrEqual(t, k, 0, "timestamp %v not in table index", stamps.Value(i))
			for j := range len(writerTableColumns(wt)) {
				cd, err := wt.GetData(j)
				require.NoError(t, err)
				assertArrowCellEqualsWriterCell(t, rec.Column(2+j), i, cd, k)
			}
		}
	}
}

// rowOffsetAt returns the index position of ts in the table, or -1.
func rowOffsetAt(wt WriterTable, ts time.Time) int {
	for k, v := range wt.GetIndex() {
		if v.Equal(ts) {
			return k
		}
	}

	return -1
}

// writerCellIsNull reports whether the writer pushed the null sentinel for
// this cell: math.MinInt64 for int64, NaN for double, NullTime for
// timestamps.
func writerCellIsNull(cd ColumnData, k int) bool {
	switch c := cd.(type) {
	case *ColumnDataInt64:
		return c.xs[k] == math.MinInt64
	case *ColumnDataDouble:
		return math.IsNaN(c.xs[k])
	case *ColumnDataTimestamp:
		return IsNullTime(QdbTimespecSliceToTime(c.xs)[k])
	default:
		return false
	}
}

// assertArrowCellEqualsWriterCell compares one Arrow slot with the value
// the writer pushed for it. The C API drops one trailing NUL from Arrow
// string cells (the chunk path and blobs keep it); the expectation follows
// that until the C side is fixed.
func assertArrowCellEqualsWriterCell(t testHelper, arr arrow.Array, i int, cd ColumnData, k int) {
	t.Helper()

	if writerCellIsNull(cd, k) {
		assert.True(t, arr.IsNull(i), "slot %d is null", i)

		return
	}

	require.True(t, arr.IsValid(i), "slot %d is valid", i)
	switch c := cd.(type) {
	case *ColumnDataInt64:
		assert.Equal(t, c.xs[k], arr.(*array.Int64).Value(i))
	case *ColumnDataDouble:
		assert.Equal(t, c.xs[k], arr.(*array.Float64).Value(i))
	case *ColumnDataTimestamp:
		assert.Equal(t, QdbTimespecSliceToTime(c.xs)[k].UnixNano(), int64(arr.(*array.Timestamp).Value(i)))
	case *ColumnDataBlob:
		assert.Equal(t, c.xs[k], arr.(*array.Binary).Value(i))
	case *ColumnDataString:
		assert.Equal(t, strings.TrimSuffix(c.xs[k], "\x00"), arr.(*array.String).Value(i))
	default:
		require.Failf(t, "unexpected column data", "%T", cd)
	}
}

func TestReaderArrowEqualsPushedTablesPerType(t *testing.T) {
	for _, ctype := range []TsColumnType{TsColumnInt64, TsColumnDouble, TsColumnTimestamp, TsColumnBlob, TsColumnString, TsColumnSymbol} {
		t.Run(fmt.Sprintf("%v", ctype), func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				handle := newTestHandle(rt)

				WithGCAndHandle(rt, handle, "TestReaderArrowEqualsPushedTablesPerType", func() {
					tables := genPopulatedTablesOfType(rt, handle, ctype)
					pushWriterTables(t, handle, tables)
					if ctype == TsColumnSymbol {
						// The C API accepts a single symbol table per read.
						tables = tables[:1]
					}
					batchSize := rapid.IntRange(1, 16).Draw(rt, "batchSize")

					reader, err := NewReader(handle, NewReaderOptions().WithTables(writerTableNames(tables)).WithBatchSize(batchSize))
					require.NoError(rt, err)
					defer reader.Close()

					recs := collectArrow(rt, &reader)
					defer releaseRecords(recs)

					assert.Equal(rt, writerTablesRowCount(tables), totalRows(recs))
					for _, rec := range recs {
						assert.LessOrEqual(rt, rec.NumRows(), int64(batchSize))
					}
					assertArrowRecordsEqualWriterTables(rt, tables, recs)
				})
			})
		})
	}
}

func TestReaderArrowWithColumnsKeepsRequestedOrder(t *testing.T) {
	handle := newTestHandle(t)
	cols := generateWriterColumnsOfAllTypes()
	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, cols)
	require.NoError(t, err)
	wt, err := NewWriterTable(tbl.alias, cols)
	require.NoError(t, err)
	require.NoError(t, wt.SetIndex([]time.Time{time.Unix(10, 0).UTC(), time.Unix(20, 0).UTC()}))
	for j, col := range cols {
		require.NoError(t, wt.SetData(j, sampleColumnData(col.ColumnType, 2)))
	}
	pushWriterTables(t, handle, []WriterTable{wt})

	cases := []struct {
		name     string
		columns  []string
		expected []string
	}{
		{"data_only", []string{cols[1].ColumnName, cols[0].ColumnName}, []string{cols[1].ColumnName, cols[0].ColumnName}},
		{"specials_named", []string{"$timestamp", cols[2].ColumnName, "$table"}, []string{"$timestamp", cols[2].ColumnName, "$table"}},
		{"specials_only", []string{"$table", "$timestamp"}, []string{"$table", "$timestamp"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewReader(handle, NewReaderOptions().WithTables([]string{wt.GetName()}).WithColumns(tc.columns))
			require.NoError(t, err)
			defer reader.Close()

			recs := collectArrow(t, &reader)
			defer releaseRecords(recs)

			require.NotEmpty(t, recs)
			assert.Equal(t, int64(2), totalRows(recs))
			fields := recs[0].Schema().Fields()
			names := make([]string, len(fields))
			for i, f := range fields {
				names[i] = f.Name
			}
			assert.Equal(t, tc.expected, names)
		})
	}
}

// sampleColumnData builds n rows of deterministic data for ctype.
func sampleColumnData(ctype TsColumnType, n int) ColumnData { //nolint:ireturn // Justified: runtime type selection
	switch ctype {
	case TsColumnInt64:
		v := NewColumnDataInt64(make([]int64, n))

		return &v
	case TsColumnDouble:
		v := NewColumnDataDouble(make([]float64, n))

		return &v
	case TsColumnTimestamp:
		v := NewColumnDataTimestamp(make([]time.Time, n))

		return &v
	case TsColumnBlob:
		v := NewColumnDataBlob(make([][]byte, n))

		return &v
	case TsColumnString, TsColumnSymbol:
		xs := make([]string, n)
		for i := range xs {
			xs[i] = "s"
		}
		v := NewColumnDataString(xs)

		return &v
	case TsColumnUninitialized:
		return nil
	}

	return nil
}

func TestReaderArrowBatchesSurviveReaderAndHandleClose(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)
		tables := genPopulatedTables(rt, handle)
		pushWriterTables(t, handle, tables)

		reader, err := NewReader(handle, NewReaderOptions().WithTables(writerTableNames(tables)).WithBatchSize(1))
		require.NoError(rt, err)

		recs := collectArrow(rt, &reader)
		reader.Close()
		require.NoError(rt, handle.Close())
		runtime.GC()

		assert.Equal(rt, writerTablesRowCount(tables), totalRows(recs))
		assertArrowRecordsEqualWriterTables(rt, tables, recs)
		releaseRecords(recs)
	})
}

func TestReaderArrowEmptyTableYieldsNothing(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	assert.Empty(t, collectArrow(t, &reader))
}

func TestReaderArrowSecondSequenceFails(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	collectArrow(t, &reader)

	steps := 0
	for rec, err := range reader.Arrow() {
		steps++
		assert.Nil(t, rec)
		assert.ErrorIs(t, err, ErrInvalidIterator)
	}
	assert.Equal(t, 1, steps)

	for _, err := range reader.Chunks() {
		assert.ErrorIs(t, err, ErrInvalidIterator)
	}
}

func TestReaderArrowBreakThenClose(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)
		tables := genPopulatedTables(rt, handle)
		pushWriterTables(t, handle, tables)

		reader, err := NewReader(handle, NewReaderOptions().WithTables(writerTableNames(tables)).WithBatchSize(1))
		require.NoError(rt, err)

		for rec, err := range reader.Arrow() {
			require.NoError(rt, err)
			assert.Equal(rt, int64(1), rec.NumRows())
			rec.Release()

			break
		}

		reader.Close()
		reader.Close()
	})
}

func TestReaderMissingTableIsAliasNotFound(t *testing.T) {
	handle := newTestHandle(t)

	_, err := NewReader(handle, NewReaderDefaultOptions([]string{generateDefaultAlias()}))
	assert.ErrorIs(t, err, ErrAliasNotFound)
}

// TestReaderArrowLargeBatchesSurviveClose reads buffers far above any
// small-buffer path in the C stream and uses them only after the reader,
// the handle and a GC cycle are gone.
func TestReaderArrowLargeBatchesSurviveClose(t *testing.T) {
	const rows, batchSize = 20000, 4096
	handle := newTestHandle(t)
	cols := []WriterColumn{{ColumnName: "s", ColumnType: TsColumnString}, {ColumnName: "b", ColumnType: TsColumnBlob}}
	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, cols)
	require.NoError(t, err)
	wt, err := NewWriterTable(tbl.alias, cols)
	require.NoError(t, err)

	idx := make([]time.Time, rows)
	strs := make([]string, rows)
	blobs := make([][]byte, rows)
	for i := range rows {
		idx[i] = time.Unix(int64(i), 0).UTC()
		strs[i] = strings.Repeat(fmt.Sprintf("%05d", i), 40)
		blobs[i] = []byte(strings.Repeat(fmt.Sprintf("%06d", i), 50))
	}
	s, b := NewColumnDataString(strs), NewColumnDataBlob(blobs)
	require.NoError(t, wt.SetIndex(idx))
	require.NoError(t, wt.SetData(0, &s))
	require.NoError(t, wt.SetData(1, &b))
	pushWriterTables(t, handle, []WriterTable{wt})

	reader, err := NewReader(handle, NewReaderOptions().WithTables([]string{tbl.alias}).WithBatchSize(batchSize))
	require.NoError(t, err)
	recs := collectArrow(t, &reader)
	reader.Close()
	require.NoError(t, handle.Close())
	runtime.GC()
	debug.FreeOSMemory()

	require.Equal(t, int64(rows), totalRows(recs))
	seen := 0
	for _, rec := range recs {
		sa, ok := rec.Column(2).(*array.String)
		require.True(t, ok)
		ba, ok := rec.Column(3).(*array.Binary)
		require.True(t, ok)
		for i := range int(rec.NumRows()) {
			assert.Equal(t, strs[seen], sa.Value(i))
			assert.Equal(t, blobs[seen], ba.Value(i))
			seen++
		}
	}
	releaseRecords(recs)
}
