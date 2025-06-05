package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestReaderOptionsCanCreateNew(t *testing.T) {
	assert := assert.New(t)

	opts := NewReaderOptions()
	assert.Empty(opts.tables)
	assert.Empty(opts.columns)
	assert.True(opts.rangeStart.IsZero())
	assert.True(opts.rangeEnd.IsZero())
}

func TestReaderOptionsCanSetProperties(t *testing.T) {
	assert := assert.New(t)

	tables := []string{"tbl1", "tbl2"}
	columns := []string{"col1", "col2"}
	start := time.Unix(0, 0)
	end := time.Unix(10, 0)

	opts := NewReaderOptions().WithTables(tables).WithColumns(columns).WithTimeRange(start, end)

	assert.Equal(tables, opts.tables)
	assert.Equal(columns, opts.columns)
	assert.Equal(start, opts.rangeStart)
	assert.Equal(end, opts.rangeEnd)
}

func TestReaderReturnsErrorOnInvalidRange(t *testing.T) {
	assert := assert.New(t)

	handle := newTestHandle(t)
	defer handle.Close()

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
}

func TestReaderCanOpenWithValidOptions(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	handle := newTestHandle(t)
	defer handle.Close()

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
	defer reader.Close()
	assert.NoError(err)
}

func TestReaderCanReadDataFromSingleTable(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(t)
		defer handle.Close()

		tables := genPopulatedTables(rt, handle, 1)
		pushWriterTables(t, handle, tables)

		names := writerTableNames(tables)
		columns := writerTablesColumns(tables)
		columnNames := columnNamesFromWriterColumns(columns)

		opts := NewReaderOptions().WithTables(names).WithColumns(columnNames)
		reader, err := NewReader(handle, opts)
		require.NoError(rt, err)
		defer reader.Close()

		data, err := reader.FetchAll()
		require.NoError(rt, err)

		assertWriterTablesEqualReaderChunks(t, tables, names, data)
	})
}

func TestReaderCanReadDataFromMultipleTables(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(t)
		defer handle.Close()

		tableCount := rapid.IntRange(2, 8).Draw(rt, "tableCount")
		tables := genPopulatedTables(rt, handle, tableCount)
		pushWriterTables(t, handle, tables)

		names := writerTableNames(tables)
		columns := writerTablesColumns(tables)
		columnNames := columnNamesFromWriterColumns(columns)

		opts := NewReaderOptions().WithTables(names).WithColumns(columnNames)
		reader, err := NewReader(handle, opts)
		require.NoError(rt, err)
		defer reader.Close()

		data, err := reader.FetchAll()
		require.NoError(rt, err)

		assertWriterTablesEqualReaderChunks(t, tables, names, data)
	})
}

// TestReaderMergeReaderChunksPanics demonstrates that mergeReaderChunks panics
// when given valid input.
func TestReaderMergeReaderChunks(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		xs := genReaderChunks(rt)
		ret, err := mergeReaderChunks(xs)

		assert.NoError(rt, err)

		assertReaderChunksEqualChunk(t, xs, ret)
	})
}
