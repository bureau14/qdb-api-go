package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestWriterTableCreateNew(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	alias := generateDefaultAlias()
	cols := generateWriterColumns(1)
	writerTable, err := NewWriterTable(alias, cols)
	require.NoError(err)

	assert.Equal(alias, writerTable.GetName(), "table names should match")
}

func TestWriterTableCanSetIndex(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	writerTable := newTestWriterTable(t)
	require.NotNil(writerTable)

	idx := generateDefaultIndex(1024)
	writerTable.SetIndex(idx)
	assert.Equal(writerTable.GetIndex(), idx)
}

func TestWriterTableCanSetDataAllColumnNames(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	columns := generateWriterColumnsOfAllTypes()

	handle := newTestHandle(t)
	defer handle.Close()

	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
	require.NoError(err)

	writerTable, err := NewWriterTable(table.alias, columns)
	require.NoError(err)
	require.NotNil(writerTable)

	idx := generateDefaultIndex(1024)
	writerTable.SetIndex(idx)

	datas, err := generateWriterDatas(len(idx), columns)
	require.NoError(err)
	err = writerTable.SetDatas(datas)

	if assert.Nil(err) {
		for i, inData := range datas {
			outData, err := writerTable.GetData(i)
			if assert.Nil(err) {
				assert.Equal(inData, outData, "expect data arrays to be identical")
			}
		}
	}
}

func TestWriterOptionsCanCreateNew(t *testing.T) {
	assert := assert.New(t)

	options := NewWriterOptions()

	assert.Equal(WriterPushModeTransactional, options.GetPushMode())
	assert.False(options.IsDropDuplicatesEnabled())
	assert.Empty(options.GetDropDuplicateColumns())
	assert.Equal(WriterDeduplicationModeDisabled, options.GetDeduplicationMode())
}

func TestWriterOptionsCanSetProperties(t *testing.T) {
	// Validates that we can adjust properties for the writer.
	//
	// Currently validates that:
	// - we can adjust the push mode
	// - enable deduplication, either on all columns or based on specific columns.
	//
	// When new options are introduced, test cases should be added here.
	assert := assert.New(t)

	// Create options with fast push mode
	options := NewWriterOptions().WithPushMode(WriterPushModeFast)
	assert.Equal(options.GetPushMode(), WriterPushModeFast)

	// Create options with deduplication based on all columns
	options = NewWriterOptions().EnableDropDuplicates()
	if assert.Equal(options.IsDropDuplicatesEnabled(), true) {
		// And then we also expect an empty array of to-deduplicate columns
		cols := options.GetDropDuplicateColumns()
		assert.Empty(cols)
	}

	// Create options with deduplication based on specific columns
	cols := []string{"col1", "col2"}
	options = NewWriterOptions().EnableDropDuplicatesOn(cols)
	if assert.Equal(options.IsDropDuplicatesEnabled(), true) {
		// And then we also expect an empty array of to-deduplicate columns
		cols_ := options.GetDropDuplicateColumns()
		assert.Equal(cols_, cols)
	}

	// Verify that deduplication mode defaults to drop when enabled
	assert.Equal(WriterDeduplicationModeDrop, options.GetDeduplicationMode())

	// Setting deduplication mode explicitly to upsert
	options = options.WithDeduplicationMode(WriterDeduplicationModeUpsert)
	assert.Equal(WriterDeduplicationModeUpsert, options.GetDeduplicationMode())

	// Verify write-through flag manipulation
	options = NewWriterOptions()
	assert.True(options.IsWriteThroughEnabled())

	options = options.DisableWriteThrough()
	assert.False(options.IsWriteThroughEnabled())
	assert.False(options.IsAsyncClientPushEnabled())

	// enable async first then re-enable write through
	options = options.EnableAsyncClientPush().EnableWriteThrough()
	assert.True(options.IsAsyncClientPushEnabled())
	assert.True(options.IsWriteThroughEnabled())

	// disable only write through; async should remain
	options = options.DisableWriteThrough()
	assert.False(options.IsWriteThroughEnabled())
	assert.True(options.IsAsyncClientPushEnabled())

	// disable async; write-through untouched
	options = options.DisableAsyncClientPush()
	assert.False(options.IsAsyncClientPushEnabled())
}

func TestWriterCanCreateNew(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer
	writer := NewWriterWithDefaultOptions()

	// Validate that the writer is not nil
	assert.NotNil(writer)
}

func TestWriterCanCreateWithOptions(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer with options
	writer := NewWriter(NewWriterOptions())

	// Validate that the writer is not nil
	assert.NotNil(writer)
}

func TestWriterOptionsUpsertRequiresColumns(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Writer with upsert mode but no deduplication columns
	opts := NewWriterOptions().WithDeduplicationMode(WriterDeduplicationModeUpsert)
	writer := NewWriter(opts)

	tbl := newTestWriterTable(t)
	tbl.SetIndex(generateDefaultIndex(1024))
	datas, err := generateWriterDatas(1024, tbl.columnInfoByOffset)

	require.NoError(err)
	require.NoError(tbl.SetDatas(datas))
	require.NoError(writer.SetTable(tbl))

	handle := newTestHandle(t)
	defer handle.Close()

	err = writer.Push(handle)
	assert.Error(err, "expect error when enabling upsert without columns")
}

// Tests successful addition of a table to the writer
func TestWriterCanAddTable(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create a new table
	writerTable := newTestWriterTable(t)

	// Add the table to the writer
	err := writer.SetTable(writerTable)
	require.NoError(err)

	require.Equal(writer.Length(), 1, "expect one table in the writer")

	writerTable_, err := writer.GetTable(writerTable.GetName())

	require.NoError(err)
	assert.Equal(writerTable, writerTable_, "expect tables to be identical")
}

// Tests that adding a table with the same name twice returns an error
func TestWriterCannotAddTableTwice(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create a new table
	writerTable := newTestWriterTable(t)

	// Add the table to the writer
	err := writer.SetTable(writerTable)
	if assert.NoError(err) {
		assert.Equal(writer.Length(), 1, "expect one table in the writer")

		err = writer.SetTable(writerTable)
		assert.NotNil(err, "expect error when adding the same table twice")
	}
}

// Tests that adding a table with the same name twice returns an error
func TestWriterReturnsErrorIfTableNotFound(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create a new table
	tableName := generateDefaultAlias()
	_, err := writer.GetTable(tableName)
	assert.Error(err, "expect error when getting a non-existing table")
}

func TestWriterCanAddMultipleTables(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create two new tables with identical schema
	writerTable1 := newTestWriterTable(t)
	cols := make([]WriterColumn, len(writerTable1.columnInfoByOffset))
	copy(cols, writerTable1.columnInfoByOffset)
	writerTable2, err := NewWriterTable(generateDefaultAlias(), cols)
	require.NoError(err)

	// Add the first table to the writer
	err = writer.SetTable(writerTable1)
	require.NoError(err)

	err = writer.SetTable(writerTable2)
	require.NoError(err)

	assert.Equal(writer.Length(), 2, "expect two tables in the writer")
}

// TestWriterSetTableSchemaConsistency verifies that SetTable rejects tables
// whose column schema differs from previously added tables.
func TestWriterSetTableSchemaConsistency(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	writer := newTestWriter(t)

	// First table with integer columns
	cols1 := generateWriterColumnsOfType(2, TsColumnInt64)
	tbl1, err := NewWriterTable(generateDefaultAlias(), cols1)
	require.NoError(err)
	require.NoError(writer.SetTable(tbl1))

	// Second table with a different schema
	cols2 := generateWriterColumnsOfType(2, TsColumnDouble)
	tbl2, err := NewWriterTable(generateDefaultAlias(), cols2)
	require.NoError(err)
	err = writer.SetTable(tbl2)

	assert.NotNil(err, "expect error when table schemas differ")
}

// Tests that the writer returns an error when invoking Push() without adding any tables
func TestWriterReturnsErrorIfNoTables(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Push the writer
	handle := newTestHandle(t)
	defer handle.Close()

	err := writer.Push(handle)
	assert.NotNil(err, "expect error when pushing an empty writer")
}

// Test that the batch writer can push into a table without issues.
// TestWriterOptionsGenerator ensures the options generator only emits valid configurations.
func TestWriterOptionsGenerator(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		opts := genWriterOptions(rt)
		require.True(rt, opts.IsValid())
	})
}

// TestWriterCanPushTables verifies the writer can push one or more tables using random options.
func TestWriterCanPushTables(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(t)
		defer handle.Close()

		tables := genPopulatedTables(rt, handle)

		writer := NewWriter(genWriterOptions(rt))
		for _, wt := range tables {
			require.NoError(rt, writer.SetTable(wt))
		}

		require.NoError(rt, writer.Push(handle))
	})
}

func TestWriterCanDeduplicate(t *testing.T) {
	handle := newTestHandle(t)
	defer handle.Close()

	rapid.Check(t, func(rt *rapid.T) {
		assert := assert.New(rt)
		require := require.New(rt)

		tables := genPopulatedTables(rt, handle)

		names := writerTableNames(tables)
		columns := writerTablesColumns(tables)
		columnNames := columnNamesFromWriterColumns(columns)

		// Initial push without deduplication.
		writer := NewWriterWithDefaultOptions()
		for _, wt := range tables {
			require.NoError(writer.SetTable(wt))
		}
		require.NoError(writer.Push(handle))

		opts := NewReaderOptions().WithTables(names).WithColumns(columnNames)
		reader, err := NewReader(handle, opts)
		require.NoError(err)
		defer reader.Close()

		baseData, err := reader.FetchAll()
		require.NoError(err)
		assertWriterTablesEqualReaderChunks(rt, tables, names, baseData)

		// Push again with deduplication enabled.
		writer = NewWriter(NewWriterOptions().EnableDropDuplicates())
		for _, wt := range tables {
			require.NoError(writer.SetTable(wt))
		}
		require.NoError(writer.Push(handle))

		reader2, err := NewReader(handle, opts)
		require.NoError(err)
		defer reader2.Close()
		dedupData, err := reader2.FetchAll()
		require.NoError(err)

		assertWriterTablesEqualReaderChunks(rt, tables, names, dedupData)

		// Push once more without deduplication.
		writer = NewWriterWithDefaultOptions()
		for _, wt := range tables {
			require.NoError(writer.SetTable(wt))
		}
		require.NoError(writer.Push(handle))

		// Verify each table now contains twice the original rows.
		for _, wt := range tables {
			rcOpts := NewReaderOptions().WithTables([]string{wt.GetName()}).WithColumns(columnNames)
			r, err := NewReader(handle, rcOpts)
			require.NoError(err)
			data, err := r.FetchAll()
			r.Close()
			require.NoError(err)

			assert.Equal(wt.RowCount()*2, data.RowCount())
		}
	})
}

// TestWriterCGOSafety validates that our CGO implementation works correctly
// with strict CGO checking enabled (GOEXPERIMENT=cgocheck2).
// This test exercises all column types to ensure proper memory management.
func TestWriterCGOSafety(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	h := newTestHandle(t)
	defer h.Close()

	w := NewWriterWithDefaultOptions()

	// Create table with all column types
	cols := []WriterColumn{
		{ColumnName: "int_col", ColumnType: TsColumnInt64},
		{ColumnName: "double_col", ColumnType: TsColumnDouble},
		{ColumnName: "string_col", ColumnType: TsColumnString},
		{ColumnName: "blob_col", ColumnType: TsColumnBlob},
		{ColumnName: "timestamp_col", ColumnType: TsColumnTimestamp},
	}

	// Create the table in QuasarDB first
	tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
	require.NoError(err)

	table, err := NewWriterTable(tsTable.alias, cols)
	require.NoError(err)

	// Set index with a single timestamp
	now := time.Now()
	table.SetIndex([]time.Time{now})

	// Set data for each column type
	table.SetData(0, &ColumnDataInt64{xs: []int64{42}})
	table.SetData(1, &ColumnDataDouble{xs: []float64{3.14}})
	table.SetData(2, &ColumnDataString{xs: []string{"test string"}})
	table.SetData(3, &ColumnDataBlob{xs: [][]byte{[]byte("blob data")}})
	timestampData := NewColumnDataTimestamp([]time.Time{now})
	table.SetData(4, &timestampData)

	err = w.SetTable(table)
	require.NoError(err)

	// This should pass with GOEXPERIMENT=cgocheck2
	// The test validates that no Go pointers are incorrectly passed to C
	err = w.Push(h)
	assert.NoError(err, "Push should succeed with strict CGO checking")

	// Verify data was written correctly by reading it back
	opts := NewReaderOptions().
		WithTables([]string{tsTable.alias}).
		WithColumns([]string{"int_col", "double_col", "string_col", "blob_col", "timestamp_col"})

	reader, err := NewReader(h, opts)
	require.NoError(err)
	defer reader.Close()

	data, err := reader.FetchAll()
	require.NoError(err)
	assert.Equal(1, data.RowCount(), "Should have written 1 row")
}

// TestWriterEmptyData tests edge case with empty tables to ensure
// CGO safety is maintained even with no data.
func TestWriterEmptyData(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	h := newTestHandle(t)
	defer h.Close()

	w := NewWriterWithDefaultOptions()

	// Test with empty table
	cols := []WriterColumn{
		{ColumnName: "col1", ColumnType: TsColumnInt64},
	}

	// Create the table in QuasarDB first
	tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
	require.NoError(err)

	table, err := NewWriterTable(tsTable.alias, cols)
	require.NoError(err)
	// No data set - table is empty

	err = w.SetTable(table)
	require.NoError(err)

	err = w.Push(h)
	// Should handle empty data gracefully - writer requires at least one row
	assert.Error(err, "Should error when pushing empty table")
}

// TestWriterLargeData tests with larger datasets to ensure no CGO issues at scale.
// This test is particularly important for validating memory management and
// pointer safety with bulk operations.
func TestWriterLargeData(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	h := newTestHandle(t)
	defer h.Close()

	w := NewWriterWithDefaultOptions()

	// Create large dataset
	const rowCount = 10000
	timestamps := make([]time.Time, rowCount)
	values := make([]float64, rowCount)

	baseTime := time.Now()
	for i := range rowCount {
		timestamps[i] = baseTime.Add(time.Duration(i) * time.Second)
		values[i] = float64(i) * 1.23
	}

	cols := []WriterColumn{
		{ColumnName: "value", ColumnType: TsColumnDouble},
	}

	// Create the table in QuasarDB first
	tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
	require.NoError(err)

	table, err := NewWriterTable(tsTable.alias, cols)
	require.NoError(err)

	table.SetIndex(timestamps)
	table.SetData(0, &ColumnDataDouble{xs: values})

	err = w.SetTable(table)
	require.NoError(err)

	// This tests that our CGO implementation can handle large amounts
	// of data without violating pointer safety rules
	err = w.Push(h)
	assert.NoError(err, "Push should succeed with large dataset")

	// Verify we can read the data back
	opts := NewReaderOptions().
		WithTables([]string{tsTable.alias}).
		WithColumns([]string{"value"})

	reader, err := NewReader(h, opts)
	require.NoError(err)
	defer reader.Close()

	data, err := reader.FetchAll()
	require.NoError(err)
	assert.Equal(rowCount, data.RowCount(), "Should have written all rows")
}

// TestWriterMixedStringLengths tests string columns with various lengths
// to ensure proper memory handling for variable-length data.
// This is important for CGO safety as strings require special handling.
func TestWriterMixedStringLengths(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	h := newTestHandle(t)
	defer h.Close()

	w := NewWriterWithDefaultOptions()

	// Create strings of various lengths including empty strings
	strings := []string{
		"",            // empty string
		"a",           // single char
		"hello world", // normal string
		"a very long string that contains many characters to test memory allocation", // long string
		"string with unicode: 你好世界 🌍",                                                // unicode string
	}

	timestamps := make([]time.Time, len(strings))
	baseTime := time.Now()
	for i := range timestamps {
		timestamps[i] = baseTime.Add(time.Duration(i) * time.Second)
	}

	cols := []WriterColumn{
		{ColumnName: "text", ColumnType: TsColumnString},
	}

	// Create the table in QuasarDB first
	tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
	require.NoError(err)

	table, err := NewWriterTable(tsTable.alias, cols)
	require.NoError(err)

	table.SetIndex(timestamps)
	table.SetData(0, &ColumnDataString{xs: strings})

	err = w.SetTable(table)
	require.NoError(err)

	// This tests string handling with various lengths
	err = w.Push(h)
	assert.NoError(err, "Push should succeed with mixed string lengths")

	// Verify data integrity
	opts := NewReaderOptions().
		WithTables([]string{tsTable.alias}).
		WithColumns([]string{"text"})

	reader, err := NewReader(h, opts)
	require.NoError(err)
	defer reader.Close()

	data, err := reader.FetchAll()
	require.NoError(err)
	assert.Equal(len(strings), data.RowCount(), "Should have written all string rows")
}

// TestWriterMixedBlobSizes tests blob columns with various sizes
// to ensure proper memory handling for binary data.
// This complements the string test for variable-length data types.
func TestWriterMixedBlobSizes(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	h := newTestHandle(t)
	defer h.Close()

	w := NewWriterWithDefaultOptions()

	// Create blobs of various sizes including empty blobs
	blobs := [][]byte{
		{},                             // empty blob
		{0x00},                         // single byte
		{0x01, 0x02, 0x03, 0x04, 0x05}, // small blob
		make([]byte, 1024),             // 1KB blob
		make([]byte, 16*1024),          // 16KB blob
	}

	// Fill larger blobs with pattern data
	for i := range blobs[3] {
		blobs[3][i] = byte(i % 256)
	}
	for i := range blobs[4] {
		blobs[4][i] = byte((i * 3) % 256)
	}

	timestamps := make([]time.Time, len(blobs))
	baseTime := time.Now()
	for i := range timestamps {
		timestamps[i] = baseTime.Add(time.Duration(i) * time.Second)
	}

	cols := []WriterColumn{
		{ColumnName: "data", ColumnType: TsColumnBlob},
	}

	// Create the table in QuasarDB first
	tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
	require.NoError(err)

	table, err := NewWriterTable(tsTable.alias, cols)
	require.NoError(err)

	table.SetIndex(timestamps)
	table.SetData(0, &ColumnDataBlob{xs: blobs})

	err = w.SetTable(table)
	require.NoError(err)

	// This tests blob handling with various sizes
	err = w.Push(h)
	assert.NoError(err, "Push should succeed with mixed blob sizes")

	// Verify data integrity
	opts := NewReaderOptions().
		WithTables([]string{tsTable.alias}).
		WithColumns([]string{"data"})

	reader, err := NewReader(h, opts)
	require.NoError(err)
	defer reader.Close()

	data, err := reader.FetchAll()
	require.NoError(err)
	assert.Equal(len(blobs), data.RowCount(), "Should have written all blob rows")
}

// TestWriterEdgeCases tests empty data handling and nil safety for all column types.
// This is a high-level integration test focusing on edge cases that provide real user value.
func TestWriterEdgeCases(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	h := newTestHandle(t)
	defer h.Close()

	testCases := []struct {
		name         string
		columnType   TsColumnType
		createEmpty  func() ColumnData
		createSample func() ColumnData
	}{
		{
			name:         "Int64",
			columnType:   TsColumnInt64,
			createEmpty:  func() ColumnData { return &ColumnDataInt64{xs: []int64{}} },
			createSample: func() ColumnData { return &ColumnDataInt64{xs: []int64{42}} },
		},
		{
			name:         "Double",
			columnType:   TsColumnDouble,
			createEmpty:  func() ColumnData { return &ColumnDataDouble{xs: []float64{}} },
			createSample: func() ColumnData { return &ColumnDataDouble{xs: []float64{3.14}} },
		},
		{
			name:       "Timestamp",
			columnType: TsColumnTimestamp,
			createEmpty: func() ColumnData {
				c := NewColumnDataTimestamp([]time.Time{})
				return &c
			},
			createSample: func() ColumnData {
				c := NewColumnDataTimestamp([]time.Time{time.Now()})
				return &c
			},
		},
		{
			name:         "Blob",
			columnType:   TsColumnBlob,
			createEmpty:  func() ColumnData { return &ColumnDataBlob{xs: [][]byte{}} },
			createSample: func() ColumnData { return &ColumnDataBlob{xs: [][]byte{{0x01, 0x02}}} },
		},
		{
			name:         "String",
			columnType:   TsColumnString,
			createEmpty:  func() ColumnData { return &ColumnDataString{xs: []string{}} },
			createSample: func() ColumnData { return &ColumnDataString{xs: []string{"test"}} },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"_EmptyData", func(t *testing.T) {
			// Test empty data handling through writer.Push()
			cols := []WriterColumn{
				{ColumnName: "data", ColumnType: tc.columnType},
			}

			// Create table in QuasarDB
			tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
			require.NoError(err)

			table, err := NewWriterTable(tsTable.alias, cols)
			require.NoError(err)

			// Set empty data
			err = table.SetData(0, tc.createEmpty())
			require.NoError(err)

			w := NewWriterWithDefaultOptions()
			err = w.SetTable(table)
			require.NoError(err)

			// Push should handle empty data gracefully
			err = w.Push(h)
			// Writer requires at least one row, so empty data should error
			assert.Error(err, "Should error when pushing empty data for "+tc.name)
		})

		t.Run(tc.name+"_NilSafety", func(t *testing.T) {
			// Test nil handle scenarios - should panic
			cols := []WriterColumn{
				{ColumnName: "data", ColumnType: tc.columnType},
			}

			// Create table in QuasarDB
			tsTable, err := createTableOfWriterColumnsAndDefaultShardSize(h, cols)
			require.NoError(err)

			table, err := NewWriterTable(tsTable.alias, cols)
			require.NoError(err)

			// Set some data and index
			table.SetIndex([]time.Time{time.Now()})
			err = table.SetData(0, tc.createSample())
			require.NoError(err)

			w := NewWriterWithDefaultOptions()
			err = w.SetTable(table)
			require.NoError(err)

			// Test pushing with nil handle - should panic due to qdbCopyString
			assert.Panics(func() {
				_ = w.Push(HandleType{})
			}, "Should panic when pushing with nil handle for "+tc.name)
		})
	}
}
