package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestReaderOptionsCanCreateNew(t *testing.T) {
	a := assert.New(t)

	opts := NewReaderOptions()
	a.Empty(opts.tables)
	a.Empty(opts.columns)
	a.True(opts.rangeStart.IsZero())
	a.True(opts.rangeEnd.IsZero())
}

func TestReaderOptionsCanSetProperties(t *testing.T) {
	a := assert.New(t)

	tables := []string{"tbl1", "tbl2"}
	columns := []string{"col1", "col2"}
	start := time.Unix(0, 0)
	end := time.Unix(10, 0)

	opts := NewReaderOptions().WithTables(tables).WithColumns(columns).WithTimeRange(start, end)

	a.Equal(tables, opts.tables)
	a.Equal(columns, opts.columns)
	a.Equal(start, opts.rangeStart)
	a.Equal(end, opts.rangeEnd)
}

func TestReaderReturnsErrorOnInvalidRange(t *testing.T) {
	handle := newTestHandle(t)

	WithGCAndHandle(t, handle, "TestReaderReturnsErrorOnInvalidRange", func() {
		assert := assert.New(t)

		// Error when no range provided
		opts := NewReaderOptions().WithTables([]string{"table1"})
		_, err := NewReader(handle, opts)
		assert.Error(err)

		// Error when range end precedes start
		opts = opts.WithTimeRange(time.Unix(10, 0), time.Unix(5, 0))
		_, err = NewReader(handle, opts)
		assert.Error(err)

		// Error when start is zero but end is non-zero
		opts = opts.WithTimeRange(time.Time{}, time.Unix(5, 0))
		_, err = NewReader(handle, opts)
		assert.Error(err)

		// Error when start is non-zero but end is zero
		opts = opts.WithTimeRange(time.Unix(5, 0), time.Time{})
		_, err = NewReader(handle, opts)
		assert.Error(err)
	})
}

func TestReaderCanOpenWithValidOptions(t *testing.T) {
	handle := newTestHandle(t)

	WithGCAndHandle(t, handle, "TestReaderCanOpenWithValidOptions", func() {
		assert := assert.New(t)
		require := require.New(t)

		// Use all the column types we have
		columnInfos := generateColumnInfosOfAllTypes()

		// Ensure a certain table exists
		table, err := createTableOfColumnInfosAndDefaultShardSize(handle, columnInfos)
		require.NoError(err)

		// Collect column names for reader
		var columnNames []string
		for _, info := range columnInfos {
			columnNames = append(columnNames, info.Name())
		}

		// Reader should open with valid options: all columns and full time range
		opts := NewReaderOptions().
			WithTables([]string{table.Name()}).
			WithColumns(columnNames)

		reader, err := NewReader(handle, opts)
		assert.NoError(err)
		defer reader.Close()
	})
}

func TestReaderCanReadDataFromTables(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)
		// Cleanup handled automatically by newTestHandle()

		WithGCAndHandle(rt, handle, "TestReaderCanReadDataFromTables", func() {
			tables := genPopulatedTables(rt, handle)

			pushWriterTables(t, handle, tables)

			names := writerTableNames(tables)

			opts := NewReaderOptions().WithTables(names)
			reader, err := NewReader(handle, opts)
			require.NoError(rt, err)
			defer reader.Close()

			data, err := reader.FetchAll()
			require.NoError(rt, err)

			assertWriterTablesEqualReaderChunks(rt, tables, names, data)
		})
	})
}

// TestReaderMergeReaderChunksPanics demonstrates that mergeReaderChunks panics
// when given valid input.
func TestReaderMergeReaderChunks(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		WithGC(rt, "TestReaderMergeReaderChunks", func() {
			xs := genReaderChunks(rt)
			ret, err := mergeReaderChunks(xs)

			assert.NoError(rt, err)

			assertReaderChunksEqualChunk(rt, xs, ret)
		})
	})
}

func TestReaderMergeReaderChunksKeepsFirstChunk(t *testing.T) {
	cols := []ReaderColumn{{columnName: "a", columnType: TsColumnInt64}, {columnName: "b", columnType: TsColumnString}}
	idx := []time.Time{time.Unix(1, 0), time.Unix(2, 0)}

	first, err := NewReaderChunk(cols, idx, []ColumnData{ptrInt64(1, 2), ptrString("x", "y")})
	require.NoError(t, err)
	second, err := NewReaderChunk(cols, idx, []ColumnData{ptrInt64(3, 4), ptrString("z", "w")})
	require.NoError(t, err)

	merged, err := mergeReaderChunks([]ReaderChunk{first, second})
	require.NoError(t, err)

	assert.Equal(t, 4, merged.RowCount())
	for i, col := range merged.data {
		assert.Equal(t, 4, col.Length(), "column %d", i)
	}
	assert.Equal(t, 2, first.data[0].Length(), "first chunk is left intact")
}

func ptrInt64(xs ...int64) *ColumnDataInt64 {
	v := NewColumnDataInt64(xs)

	return &v
}

func ptrString(xs ...string) *ColumnDataString {
	v := NewColumnDataString(xs)

	return &v
}

// collectChunks drains a Chunks sequence, failing on any error step.
func collectChunks(t testHelper, r *Reader) []ReaderChunk {
	t.Helper()

	var chunks []ReaderChunk
	for chunk, err := range r.Chunks() {
		require.NoError(t, err)
		chunks = append(chunks, chunk)
	}

	return chunks
}

func TestReaderChunksEqualsPushedTables(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)

		WithGCAndHandle(rt, handle, "TestReaderChunksEqualsPushedTables", func() {
			tables := genPopulatedTables(rt, handle)
			pushWriterTables(t, handle, tables)
			names := writerTableNames(tables)
			batchSize := rapid.IntRange(1, 16).Draw(rt, "batchSize")

			reader, err := NewReader(handle, NewReaderOptions().WithTables(names).WithBatchSize(batchSize))
			require.NoError(rt, err)
			defer reader.Close()

			chunks := collectChunks(rt, &reader)
			for _, chunk := range chunks {
				assert.LessOrEqual(rt, chunk.RowCount(), batchSize)
			}

			merged, err := mergeReaderChunks(chunks)
			require.NoError(rt, err)
			assertWriterTablesEqualReaderChunks(rt, tables, names, merged)
		})
	})
}

func TestReaderChunksEmptyTableYieldsNothing(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	assert.Empty(t, collectChunks(t, &reader))
}

func TestReaderChunksSecondSequenceFails(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	collectChunks(t, &reader)

	steps := 0
	for _, err := range reader.Chunks() {
		steps++
		assert.ErrorIs(t, err, ErrInvalidIterator)
	}
	assert.Equal(t, 1, steps, "second sequence yields exactly one error step")

	assert.False(t, reader.Next())
	assert.ErrorIs(t, reader.Err(), ErrInvalidIterator)
}

func TestReaderNextThenChunksFails(t *testing.T) {
	handle := newTestHandle(t)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, generateWriterColumnsOfAllTypes())
	require.NoError(t, err)

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{tbl.alias}))
	require.NoError(t, err)
	defer reader.Close()

	assert.False(t, reader.Next())
	require.NoError(t, reader.Err())

	for _, err := range reader.Chunks() {
		assert.ErrorIs(t, err, ErrInvalidIterator)
	}
}

func TestReaderChunksBreakThenClose(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)

		WithGCAndHandle(rt, handle, "TestReaderChunksBreakThenClose", func() {
			tables := genPopulatedTables(rt, handle)
			pushWriterTables(t, handle, tables)

			reader, err := NewReader(handle, NewReaderOptions().WithTables(writerTableNames(tables)).WithBatchSize(1))
			require.NoError(rt, err)

			for chunk, err := range reader.Chunks() {
				require.NoError(rt, err)
				assert.Equal(rt, 1, chunk.RowCount())

				break
			}

			reader.Close()
			reader.Close()
		})
	})
}
