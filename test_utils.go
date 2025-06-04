package qdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

const (
	clusterPublicKeyFile string = "cluster_public.key"
	userPrivateKeyFile   string = "user_private.key"
	usersConfigFile      string = "users.cfg"

	insecureURI string = "qdb://127.0.0.1:2836"
	secureURI   string = "qdb://127.0.0.1:2838"
)

func newTestHandle(t *testing.T) HandleType {
	t.Helper()

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(t, err)

	return handle
}

// fixture for creating a default test WriterTable
func newTestWriterTable(t *testing.T) WriterTable {
	t.Helper()

	tableName := generateDefaultAlias()
	columns := generateWriterColumnsOfAllTypes()

	writerTable, err := NewWriterTable(tableName, columns)
	require.NoError(t, err)
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

// createAndPopulateTables creates `tableCount` tables with identical schema and
// fills them with `rowCount` rows of random data. It returns the prepared
// WriterTable instances, their table names, and the column schema used.
func createAndPopulateTables(t *testing.T, handle HandleType, tableCount, rowCount int) ([]WriterTable, []string, []WriterColumn) {
	t.Helper()

	columns := generateWriterColumnsOfAllTypes()
	idx := generateDefaultIndex(rowCount)
	datas, err := generateWriterDatas(rowCount, columns)
	require.NoError(t, err)

	tables := make([]WriterTable, tableCount)
	names := make([]string, tableCount)

	for i := 0; i < tableCount; i++ {
		tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
		require.NoError(t, err)

		wt, err := NewWriterTable(tbl.alias, columns)
		require.NoError(t, err)
		wt.SetIndex(idx)
		require.NoError(t, wt.SetDatas(datas))

		tables[i] = wt
		names[i] = tbl.Name()
	}

	return tables, names, columns
}

// pushWriterTables writes the provided tables to the server using a writer with
// default options. Any error will fail the test via require.
func pushWriterTables(t *testing.T, handle HandleType, tables []WriterTable) {
	t.Helper()

	writer := NewWriterWithDefaultOptions()
	require.NotNil(t, writer)

	for _, wt := range tables {
		require.NoError(t, writer.SetTable(wt))
	}

	require.NoError(t, writer.Push(handle))
}

// columnNamesFromWriterColumns extracts the column names from the provided
// WriterColumn definitions.
func columnNamesFromWriterColumns(cols []WriterColumn) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.ColumnName
	}
	return names
}

// assertWriterTablesEqualReaderBatch compares the data written via WriterTables
// with the data returned by the Reader.
func assertWriterTablesEqualReaderBatch(t *testing.T, expected []WriterTable, names []string, got ReaderBatch) {
	t.Helper()

	require := require.New(t)
	assert := assert.New(t)

	require.Equal(len(expected), len(got))
	require.Equal(len(expected), len(names))
	for i, wt := range expected {
		rc := got[i]

		expectedName := names[i]
		assert.Equal(expectedName, rc.tableName)
		assert.Equal(wt.GetIndex(), rc.idx)

		// Validate row count information at least matches the writer input
		assert.Equal(len(wt.GetIndexAsNative()), rc.rowCount)

		offset := len(rc.columnInfoByOffset) - len(wt.columnInfoByOffset)
		require.GreaterOrEqual(offset, 0)
		for j, col := range wt.columnInfoByOffset {
			rcCol := rc.columnInfoByOffset[j+offset]
			assert.Equal(col.ColumnName, rcCol.columnName)
			assert.Equal(col.ColumnType, rcCol.columnType)

			expectedData := wt.data[j]
			gotData := rc.data[j+offset]
			switch col.ColumnType {
			case TsColumnInt64:
				exp, err := GetInt64Array(expectedData)
				require.NoError(err)
				gotVals, err := GetReaderDataInt64(gotData)
				require.NoError(err)
				assert.Equal(exp.xs, gotVals)
			case TsColumnDouble:
				exp, err := GetDoubleArray(expectedData)
				require.NoError(err)
				gotVals, err := GetReaderDataDouble(gotData)
				require.NoError(err)
				assert.Equal(exp.xs, gotVals)
			case TsColumnTimestamp:
				exp, err := GetTimestampArray(expectedData)
				require.NoError(err)
				gotVals, err := GetReaderDataTimestamp(gotData)
				require.NoError(err)
				require.Equal(len(exp.xs), len(gotVals))
				for k, v := range exp.xs {
					assert.Equal(QdbTimespecToTime(v), gotVals[k])
				}
			case TsColumnBlob:
				exp, err := GetBlobArray(expectedData)
				require.NoError(err)
				gotVals, err := GetReaderDataBlob(gotData)
				require.NoError(err)
				assert.Equal(exp.xs, gotVals)
			case TsColumnString:
				exp, err := GetStringArray(expectedData)
				require.NoError(err)
				gotVals, err := GetReaderDataString(gotData)
				require.NoError(err)
				assert.Equal(exp.xs, gotVals)
			default:
				t.Fatalf("unsupported column type %v", col.ColumnType)
			}
		}
	}
}

// genTime generates a random UTC time as used in time_test.go.
func genTime(t *rapid.T) time.Time {
	sec := rapid.Int64Range(0, 17_179_869_184).Draw(t, "sec")
	nsec := rapid.Int64Range(0, 999_999_999).Draw(t, "nsec")
	return time.Unix(sec, nsec).UTC()
}

// genReaderChunk constructs a ReaderChunk with random int64 columns.
// The chunk will contain between 1 and 1024 rows and up to 8 columns.
func genReaderChunk(t *rapid.T) ReaderChunk {
	rows := rapid.IntRange(1, 1024).Draw(t, "rows")
	cols := rapid.IntRange(1, 8).Draw(t, "cols")

	idx := make([]time.Time, rows)
	for i := 0; i < rows; i++ {
		idx[i] = genTime(t)
	}

	data := make([]ReaderData, cols)
	for c := 0; c < cols; c++ {
		values := make([]int64, rows)
		for r := 0; r < rows; r++ {
			values[r] = rapid.Int64().Draw(t, fmt.Sprintf("val_%d_%d", c, r))
		}
		rd := newReaderDataInt64(fmt.Sprintf("c%d", c), values)
		data[c] = &rd
	}

	return ReaderChunk{
		tableName: "tbl",
		rowCount:  rows,
		idx:       idx,
		data:      data,
	}
}

// genReaderChunks returns between 1 and 8 ReaderChunks that all share the same
// schema.
func genReaderChunks(t *rapid.T) []ReaderChunk {
	count := rapid.IntRange(1, 8).Draw(t, "chunkCount")
	rows := rapid.IntRange(1, 1024).Draw(t, "rows")
	cols := rapid.IntRange(1, 8).Draw(t, "cols")

	makeChunk := func() ReaderChunk {
		idx := make([]time.Time, rows)
		for i := 0; i < rows; i++ {
			idx[i] = genTime(t)
		}

		data := make([]ReaderData, cols)
		for c := 0; c < cols; c++ {
			values := make([]int64, rows)
			for r := 0; r < rows; r++ {
				values[r] = rapid.Int64().Draw(t, fmt.Sprintf("v_%d_%d", c, r))
			}
			rd := newReaderDataInt64(fmt.Sprintf("c%d", c), values)
			data[c] = &rd
		}

		return ReaderChunk{tableName: "tbl", rowCount: rows, idx: idx, data: data}
	}

	out := make([]ReaderChunk, count)
	for i := 0; i < count; i++ {
		out[i] = makeChunk()
	}
	return out
}

// genReaderBatch generates a single ReaderBatch with a consistent schema.
func genReaderBatch(t *rapid.T) ReaderBatch {
	count := rapid.IntRange(2, 8).Draw(t, "chunkCount")
	rows := rapid.IntRange(1, 1024).Draw(t, "rows")
	cols := rapid.IntRange(1, 8).Draw(t, "cols")

	makeChunk := func() ReaderChunk {
		idx := make([]time.Time, rows)
		for i := 0; i < rows; i++ {
			idx[i] = genTime(t)
		}

		data := make([]ReaderData, cols)
		for c := 0; c < cols; c++ {
			values := make([]int64, rows)
			for r := 0; r < rows; r++ {
				values[r] = rapid.Int64().Draw(t, fmt.Sprintf("b_v_%d_%d", c, r))
			}
			rd := newReaderDataInt64(fmt.Sprintf("c%d", c), values)
			data[c] = &rd
		}

		return ReaderChunk{tableName: "tbl", rowCount: rows, idx: idx, data: data}
	}

	batch := make([]ReaderChunk, count)
	for i := 0; i < count; i++ {
		batch[i] = makeChunk()
	}
	return batch
}

// genReaderBatches returns up to 8 ReaderBatch values, all sharing the same shape.
func genReaderBatches(t *rapid.T) []ReaderBatch {
	batchCount := rapid.IntRange(2, 4).Draw(t, "batchCount")
	chunkCount := rapid.IntRange(batchCount+1, batchCount+4).Draw(t, "chunkCount")

	rows := rapid.IntRange(1, 1024).Draw(t, "rows")
	cols := rapid.IntRange(1, 8).Draw(t, "cols")

	makeChunk := func() ReaderChunk {
		idx := make([]time.Time, rows)
		for i := 0; i < rows; i++ {
			idx[i] = genTime(t)
		}
		data := make([]ReaderData, cols)
		for c := 0; c < cols; c++ {
			values := make([]int64, rows)
			for r := 0; r < rows; r++ {
				values[r] = rapid.Int64().Draw(t, fmt.Sprintf("b_v_%d_%d", c, r))
			}
			rd := newReaderDataInt64(fmt.Sprintf("c%d", c), values)
			data[c] = &rd
		}
		return ReaderChunk{tableName: "tbl", rowCount: rows, idx: idx, data: data}
	}

	batches := make([]ReaderBatch, batchCount)
	for b := 0; b < batchCount; b++ {
		batch := make([]ReaderChunk, chunkCount)
		for c := 0; c < chunkCount; c++ {
			batch[c] = makeChunk()
		}
		batches[b] = batch
	}
	return batches
}
