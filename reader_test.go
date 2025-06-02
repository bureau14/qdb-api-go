package qdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderMergeChunksColumnLengthMismatch(t *testing.T) {
	chunk1 := sampleReaderChunk("table1", time.Unix(100, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{10}},
	})

	chunk2 := sampleReaderChunk("table1", time.Unix(200, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{20}},
		&ReaderDataDouble{name: "col2", xs: []float64{30.0}},
	})

	_, err := mergeReaderChunks([]ReaderChunk{chunk1, chunk2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column length mismatch")
}

func TestReaderMergeChunksColumnMismatch(t *testing.T) {
	chunk1 := sampleReaderChunk("table1", time.Unix(100, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{10}},
	})

	chunk2 := sampleReaderChunk("table1", time.Unix(200, 0), []ReaderColumn{
		&ReaderDataDouble{name: "col2", xs: []float64{20.5}}, // different column type
	})

	_, err := mergeReaderChunks([]ReaderChunk{chunk1, chunk2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column mismatch")
}

func TestReaderMergeChunksTableMismatch(t *testing.T) {
	chunk1 := sampleReaderChunk("table1", time.Unix(100, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{10}},
	})

	chunk2 := sampleReaderChunk("table2", time.Unix(200, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{20}},
	})

	_, err := mergeReaderChunks([]ReaderChunk{chunk1, chunk2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table name mismatch")
}

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
	defer reader.Close(handle)
	assert.NoError(err)
}

func TestReaderCanReadDataFromSingleTable(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(err)
	defer handle.Close()

	// Step 1: create table and fill with data using the `Writer`. Look
	//         at writer_test.go to see how to use the Writer to fill a table with
	//         data.

	// Step 2: initialize the reader on this table

	// Step 3: fetch all data from reader

	// Step 4: assert that there's just a single table being returned

	// Step 5: assert that each column's data matches exactly what we have written into
	//         it

}

func sampleReaderChunk(tblName string, timestamp time.Time, columnDefs []ReaderColumn) ReaderChunk {
	return ReaderChunk{
		tableName:          tblName,
		timestamps:         []time.Time{timestamp},
		columns:            columnDefs,
		columnInfoByOffset: map[uint64]TsColumnInfo{},
	}
}

func TestMergeReaderChunks(t *testing.T) {
	chunk1 := sampleReaderChunk("table1", time.Unix(100, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{10}},
	})

	chunk2 := sampleReaderChunk("table1", time.Unix(200, 0), []ReaderColumn{
		&ReaderDataInt64{name: "col1", xs: []int64{20}},
	})

	mergedChunk, err := mergeReaderChunks([]ReaderChunk{chunk1, chunk2})
	require.NoError(t, err)
	assert.Equal(t, "table1", mergedChunk.TableName())
	assert.Equal(t, []time.Time{time.Unix(100, 0), time.Unix(200, 0)}, mergedChunk.timestamps)

	colData, err := GetReaderDataInt64(mergedChunk.columns[0])
	require.NoError(t, err)
	assert.Equal(t, []int64{10, 20}, colData)
}
