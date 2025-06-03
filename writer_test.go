package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
func TestWriterCanPushSingleTable(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(err)
	defer handle.Close()

	// First generate the table schema + layout we will work with
	columns := generateWriterColumnsOfAllTypes()
	idx := generateDefaultIndex(1024)
	datas, err := generateWriterDatas(len(idx), columns)
	require.NoError(err)

	// Creating the table automatically assign it a name
	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
	require.NoError(err)

	// Now create a WriterTable structure and fill it
	writerTable, err := NewWriterTable(table.alias, columns)
	require.NoError(err)
	require.NotNil(writerTable)

	writerTable.SetIndex(idx)

	err = writerTable.SetDatas(datas)
	require.NoError(err)

	// And actually push the data by creating a writer, adding the table to it
	// and invoking Push().
	writer := newTestWriter(t)

	writer.SetTable(writerTable)

	// Push the writer
	err = writer.Push(handle)
	assert.NoError(err)

	// TODO: read back data using Reader, invoke `assertWriterTablesEqualReaderBatch` for comparison
}

// Test that the batch writer can push into multiple tables without issues.
func TestWriterCanPushMultipleTables(t *testing.T) {
	// TODO: implement
	//
	// implement similar test case as `TestWriterCanPushSingleTable`, but in this case
	// push into mulitple tables.
	//
	// Validate those again using the same logic with `assertWriterTablesEqualReaderBatch`
	//
	// Schemas of all tables must be identical

}
