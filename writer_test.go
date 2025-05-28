package qdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixture for creating a default test WriterTable
func newTestWriterTable(t *testing.T, numColumns int) WriterTable {
	t.Helper()

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(numColumns)

	writerTable := NewWriterTable(tableName, columns)
	require.NotNil(t, writerTable)

	return writerTable
}

// fixture for Writer creation with default options
func newTestWriter(t *testing.T) Writer {
	t.Helper()

	writer := NewWriterWithDefaultOptions()
	require.NotNil(t, writer)

	return writer
}

func TestWriterTableCreateNew(t *testing.T) {
	assert := assert.New(t)

	writerTable := newTestWriterTable(t, 1)

	assert.Equal(writerTable.GetName(), writerTable.TableName, "table names should match")
}

func TestWriterTableCanSetIndex(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	writerTable := newTestWriterTable(t, 1)
	require.NotNil(writerTable)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err)
	defer handle.Close()

	idx := generateDefaultIndex(1024)
	err = writerTable.SetIndex(handle, idx)

	if assert.Nil(err) {
		assert.Equal(writerTable.GetIndex(), idx)
	}
}

func TestWriterTableCanSetDataAllColumnNames(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	columns := generateWriterColumnsOfAllTypes()

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err, fmt.Sprintf("%v", err))
	defer handle.Close()

	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
	require.Nil(err, fmt.Sprintf("%v", err))

	writerTable := NewWriterTable(table.alias, columns)
	require.NotNil(writerTable)

	idx := generateDefaultIndex(1024)
	err = writerTable.SetIndex(handle, idx)
	require.Nil(err)

	datas, err := generateWriterDatas(handle, len(idx), columns)
	require.Nil(err)
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

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err, fmt.Sprintf("%v", err))
	defer handle.Close()

	// Writer with upsert mode but no deduplication columns
	opts := NewWriterOptions().WithDeduplicationMode(WriterDeduplicationModeUpsert)
	writer := NewWriter(opts)

	tbl := newTestWriterTable(t, 1)
	err = tbl.SetIndex(handle, generateDefaultIndex(1))
	require.Nil(err)
	datas, err := generateWriterDatas(handle, 1, tbl.columnInfoByOffset)
	require.Nil(err)
	require.Nil(tbl.SetDatas(datas))

	require.Nil(writer.SetTable(tbl))

	err = writer.Push(handle)
	assert.NotNil(err, "expect error when enabling upsert without columns")
}

// Tests successful addition of a table to the writer
func TestWriterCanAddTable(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create a new table
	writerTable := newTestWriterTable(t, 8)

	// Add the table to the writer
	err := writer.SetTable(writerTable)
	if assert.Nil(err) {
		assert.Equal(writer.Length(), 1, "expect one table in the writer")

		writerTable_, err := writer.GetTable(writerTable.GetName())

		if assert.Nil(err) {
			assert.Equal(writerTable, writerTable_, "expect tables to be identical")
		}
	}
}

// Tests that adding a table with the same name twice returns an error
func TestWriterCannotAddTableTwice(t *testing.T) {
	assert := assert.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create a new table
	writerTable := newTestWriterTable(t, 8)

	// Add the table to the writer
	err := writer.SetTable(writerTable)
	if assert.Nil(err) {
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
	assert.NotNil(err, "expect error when getting a non-existing table")
}

func TestWriterCanAddMultipleTables(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Create a new writer
	writer := newTestWriter(t)

	// Create two new tables with identical schema
	writerTable1 := newTestWriterTable(t, 8)
	cols := make([]WriterColumn, len(writerTable1.columnInfoByOffset))
	copy(cols, writerTable1.columnInfoByOffset)
	writerTable2 := NewWriterTable(generateDefaultAlias(), cols)

	// Add the first table to the writer
	err := writer.SetTable(writerTable1)
	require.Nil(err)

	err = writer.SetTable(writerTable2)
	require.Nil(err)

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
	tbl1 := NewWriterTable(generateDefaultAlias(), cols1)
	require.Nil(writer.SetTable(tbl1))

	// Second table with a different schema
	cols2 := generateWriterColumnsOfType(2, TsColumnDouble)
	tbl2 := NewWriterTable(generateDefaultAlias(), cols2)
	err := writer.SetTable(tbl2)

	assert.NotNil(err, "expect error when table schemas differ")
}

// Tests that the writer returns an error when invoking Push() without adding any tables
func TestWriterReturnsErrorIfNoTables(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err, fmt.Sprintf("%v", err))
	defer handle.Close()

	// Create a new writer
	writer := newTestWriter(t)

	// Push the writer
	err = writer.Push(handle)
	assert.NotNil(err, "expect error when pushing an empty writer")
}

// Test that the batch writer can push into a table without issues.
func TestWriterCanPushSingleTable(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err, fmt.Sprintf("%v", err))
	defer handle.Close()

	// First generate the table schema + layout we will work with
	columns := generateWriterColumnsOfType(8, TsColumnInt64)
	idx := generateDefaultIndex(1024)
	datas, err := generateWriterDatas(handle, len(idx), columns)
	require.Nil(err)

	// Creating the table automatically assign it a name
	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
	require.Nil(err, fmt.Sprintf("%v", err))

	// Now create a WriterTable structure and fill it
	writerTable := NewWriterTable(table.alias, columns)
	require.NotNil(writerTable)

	err = writerTable.SetIndex(handle, idx)
	require.Nil(err, "Unable to set index")

	err = writerTable.SetDatas(datas)
	require.Nil(err, "Unable to set data")

	// And actually push the data by creating a writer, adding the table to it
	// and invoking Push().
	writer := newTestWriter(t)

	writer.SetTable(writerTable)

	// Push the writer
	err = writer.Push(handle)
	assert.Nil(err, "Unable to push writer after setting table")
}
