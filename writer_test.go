package qdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterTableCreateNew(t *testing.T) {
	assert := assert.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	writerTable := NewWriterTable(tableName, columns)

	if assert.NotNil(writerTable) {
		assert.Equal(tableName, writerTable.GetName(), "table names should be equal")
	}
}

func TestWriterTableCanSetIndex(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	writerTable := NewWriterTable(tableName, columns)
	require.NotNil(writerTable)

	idx := generateDefaultIndex(1024)
	err := writerTable.SetIndex(idx)

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
	err = writerTable.SetIndex(idx)
	require.Nil(err)

	datas := generateWriterDatas(len(idx), columns)
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
	// Validate some default assumptions
	assert := assert.New(t)
	options := NewWriterOptions()

	// Transactional push by default
	assert.Equal(options.GetPushMode(), WriterPushModeTransactional)

	// By default deduplication is disabled
	if assert.False(options.IsDropDuplicatesEnabled()) {
		// And then we also expect an empty array of to-deduplicate columns
		cols := options.GetDropDuplicateColumns()
		assert.Empty(cols)
	}
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

// Tests successful addition of a table to the writer
func TestWriterCanAddTable(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Create a new writer
	writer := NewWriterWithDefaultOptions()
	require.NotNil(writer)

	// Create a new table
	tableName := generateDefaultAlias()
	columns := generateWriterColumns(8)
	writerTable := NewWriterTable(tableName, columns)

	// Add the table to the writer
	err := writer.SetTable(writerTable)
	if assert.Nil(err) {
		assert.Equal(writer.Length(), 1, "expect one table in the writer")

		writerTable_, err := writer.GetTable(tableName)

		if assert.Nil(err) {
			assert.Equal(writerTable, writerTable_, "expect tables to be identical")
		}
	}
}

// Tests that adding a table with the same name twice returns an error
func TestWriterCannotAddTableTwice(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Create a new writer
	writer := NewWriterWithDefaultOptions()
	require.NotNil(writer)

	// Create a new table
	tableName := generateDefaultAlias()
	writerTable := NewWriterTable(tableName, generateWriterColumns(8))

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
	require := require.New(t)

	// Create a new writer
	writer := NewWriterWithDefaultOptions()
	require.NotNil(writer)

	// Create a new table
	tableName := generateDefaultAlias()
	_, err := writer.GetTable(tableName)
	assert.NotNil(err, "expect error when getting a non-existing table")
}

func TestWriterCanAddMultipleTables(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	// Create a new writer
	writer := NewWriterWithDefaultOptions()
	require.NotNil(writer)

	// Create a new table
	tableName1 := generateDefaultAlias()
	tableName2 := generateDefaultAlias()

	columns1 := generateWriterColumns(8)
	columns2 := generateWriterColumns(8)

	writerTable1 := NewWriterTable(tableName1, columns1)
	writerTable2 := NewWriterTable(tableName2, columns2)

	// Add the first table to the writer
	err := writer.SetTable(writerTable1)
	require.Nil(err)

	err = writer.SetTable(writerTable2)
	require.Nil(err)

	assert.Equal(writer.Length(), 2, "expect two tables in the writer")
}

// Tests that the writer returns an error when invoking Push() without adding any tables
func TestWriterReturnsErrorIfNoTables(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err, fmt.Sprintf("%v", err))
	defer handle.Close()

	// Create a new writer
	writer := NewWriterWithDefaultOptions()
	require.NotNil(writer)

	// Push the writer
	err = writer.Push(handle)
	assert.NotNil(err, "expect error when pushing an empty writer")
}
