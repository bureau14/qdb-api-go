package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(err)
	defer handle.Close()

	// Error when no range provided
	opts := NewReaderOptions().WithTables([]string{"table1"})
	_, err = NewReader(handle, opts)
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

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(err)
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
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(err)
	defer handle.Close()

	// Step 1: create table and fill with data using the Writer
	columns := generateWriterColumnsOfAllTypes()
	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
	require.NoError(err)

	rowCount := 1024
	idx := generateDefaultIndex(rowCount)

	datas, err := generateWriterDatas(rowCount, columns)
	require.NoError(err)

	writerTable, err := NewWriterTable(handle, table.alias, columns)
	require.NoError(err)
	writerTable.SetIndex(idx)
	require.NoError(writerTable.SetDatas(datas))

	writer := NewWriterWithDefaultOptions()
	writer.SetTable(writerTable)
	require.NoError(writer.Push(handle))

	// Step 2: initialize the reader on this table
	var columnNames []string
	for _, c := range columns {
		columnNames = append(columnNames, c.ColumnName)
	}

	opts := NewReaderOptions().WithTables([]string{table.Name()}).WithColumns(columnNames)
	reader, err := NewReader(handle, opts)
	require.NoError(err)
	defer reader.Close()

	// Step 3: fetch all data from reader
	tables, err := reader.FetchAll()
	require.NoError(err)

	// Step 4 & 5: verify reader output matches the written data
	assertWriterTablesEqualReaderBatch(t, []WriterTable{writerTable}, []string{table.Name()}, tables)
}
