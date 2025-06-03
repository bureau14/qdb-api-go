package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
