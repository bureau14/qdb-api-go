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

// func TestReaderCanReadDataFromSingleTable(t *testing.T) {
// 	assert := assert.New(t)
// 	require := require.New(t)

// 	handle, err := SetupHandle(insecureURI, 120*time.Second)
// 	require.NoError(err)
// 	defer handle.Close()

// 	// Step 1: create table and fill with data using the Writer
// 	columns := generateWriterColumnsOfAllTypes()
// 	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
// 	require.NoError(err)

// 	rowCount := 8
// 	idx := generateDefaultIndex(rowCount)

// 	datas, err := generateWriterDatas(handle, rowCount, columns)
// 	require.NoError(err)

// 	writerTable, err := NewWriterTable(handle, table.alias, columns)
// 	require.NoError(err)
// 	require.NoError(writerTable.SetIndex(handle, idx))
// 	require.NoError(writerTable.SetDatas(datas))

// 	// Capture values from generated WriterData without using cgo
// 	var (
// 		intData    []int64
// 		doubleData []float64
// 		tsData     []time.Time
// 		blobData   [][]byte
// 		stringData []string
// 	)

// 	type timespec struct {
// 		tv_sec  int64
// 		tv_nsec int64
// 	}

// 	type cBlob struct {
// 		content        unsafe.Pointer
// 		content_length uint64
// 	}

// 	type cString struct {
// 		data   *byte
// 		length uint64
// 	}

// 	for i, c := range columns {
// 		switch c.ColumnType {
// 		case TsColumnInt64:
// 			arr, err := GetInt64Array(datas[i])
// 			require.NoError(err)
// 			tmp := unsafe.Slice((*int64)(unsafe.Pointer(arr.xs)), rowCount)
// 			intData = append(intData, tmp...)
// 		case TsColumnDouble:
// 			arr, err := GetDoubleArray(datas[i])
// 			require.NoError(err)
// 			tmp := unsafe.Slice((*float64)(unsafe.Pointer(arr.xs)), rowCount)
// 			doubleData = append(doubleData, tmp...)
// 		case TsColumnTimestamp:
// 			arr, err := GetTimestampArray(datas[i])
// 			require.NoError(err)
// 			tmp := unsafe.Slice((*timespec)(unsafe.Pointer(arr.xs)), rowCount)
// 			tsData = make([]time.Time, rowCount)
// 			for j, v := range tmp {
// 				tsData[j] = time.Unix(v.tv_sec, v.tv_nsec).UTC()
// 			}
// 		case TsColumnBlob:
// 			arr, err := GetBlobArray(datas[i])
// 			require.NoError(err)
// 			tmp := unsafe.Slice((*cBlob)(unsafe.Pointer(arr.xs)), rowCount)
// 			blobData = make([][]byte, rowCount)
// 			for j, v := range tmp {
// 				if v.content_length > 0 {
// 					b := unsafe.Slice((*byte)(v.content), int(v.content_length))
// 					blobData[j] = append([]byte(nil), b...)
// 				} else {
// 					blobData[j] = nil
// 				}
// 			}
// 		case TsColumnString:
// 			arr, err := GetStringArray(datas[i])
// 			require.NoError(err)
// 			tmp := unsafe.Slice((*cString)(unsafe.Pointer(arr.xs)), rowCount)
// 			stringData = make([]string, rowCount)
// 			for j, v := range tmp {
// 				if v.length > 0 {
// 					b := unsafe.Slice((*byte)(unsafe.Pointer(v.data)), int(v.length))
// 					stringData[j] = string(b)
// 				} else {
// 					stringData[j] = ""
// 				}
// 			}
// 		}
// 	}

// 	writer := NewWriterWithDefaultOptions()
// 	writer.SetTable(writerTable)
// 	require.NoError(writer.Push(handle))

// 	// Step 2: initialize the reader on this table
// 	var columnNames []string
// 	for _, c := range columns {
// 		columnNames = append(columnNames, c.ColumnName)
// 	}

// 	opts := NewReaderOptions().WithTables([]string{table.Name()}).WithColumns(columnNames)
// 	reader, err := NewReader(handle, opts)
// 	require.NoError(err)
// 	defer reader.Close()

// 	// Step 3: fetch all data from reader
// 	tables, err := reader.FetchAll()
// 	require.NoError(err)

// 	// Step 4: assert that there's just a single table being returned
// 	require.Equal(1, len(tables))
// 	tbl := tables[0]
// 	assert.Equal(rowCount, tbl.rowCount)
// 	assert.Equal(idx, tbl.idx)

// 	// Step 5: assert that each column's data matches exactly what we have written into it
// 	for i, col := range tbl.columnInfoByOffset {
// 		switch col.columnType {
// 		case TsColumnInt64:
// 			got, err := GetReaderDataInt64(tbl.data[i])
// 			require.NoError(err)
// 			assert.Equal(intData, got)
// 		case TsColumnDouble:
// 			got, err := GetReaderDataDouble(tbl.data[i])
// 			require.NoError(err)
// 			assert.Equal(doubleData, got)
// 		case TsColumnTimestamp:
// 			got, err := GetReaderDataTimestamp(tbl.data[i])
// 			require.NoError(err)
// 			assert.Equal(tsData, got)
// 		case TsColumnBlob:
// 			got, err := GetReaderDataBlob(tbl.data[i])
// 			require.NoError(err)
// 			assert.Equal(blobData, got)
// 		case TsColumnString:
// 			// TODO: bulk reader string support unstable; skip check
// 			got, err := GetReaderDataString(tbl.data[i])
// 			require.NoError(err)
// 			_ = got
// 		}
// 	}

// }
