package qdb

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	blobIndex      = 0
	doubleIndex    = 1
	int64Index     = 2
	stringIndex    = 3
	timestampIndex = 4
	symbolIndex    = 5
)

func TestQueryExecuteReturnsExpectedResults(t *testing.T) {
	handle := newTestHandle(t)

	td := newTestTimeseriesAllColumns(t, handle, 8)

	query := fmt.Sprintf("select * from %s in range(1970, +10d)", td.Alias)
	result, err := handle.Query(query).Execute()
	require.NoError(t, err)
	require.NotNil(t, result)
	defer handle.Release(unsafe.Pointer(result)) //nolint:gosec // Safe CGO conversion for QuasarDB API

	for rowIdx, row := range result.Rows() {
		cols := result.Columns(row)

		blob, err := cols[blobIndex+2].GetBlob()
		require.NoError(t, err)
		assert.Equal(t, td.BlobPoints[rowIdx].Content(), blob)

		dbl, err := cols[doubleIndex+2].GetDouble()
		require.NoError(t, err)
		assert.Equal(t, td.DoublePoints[rowIdx].Content(), dbl)

		i64, err := cols[int64Index+2].GetInt64()
		require.NoError(t, err)
		assert.Equal(t, td.Int64Points[rowIdx].Content(), i64)

		str, err := cols[stringIndex+2].GetString()
		require.NoError(t, err)
		assert.Equal(t, td.StringPoints[rowIdx].Content(), str)

		ts, err := cols[timestampIndex+2].GetTimestamp()
		require.NoError(t, err)
		assert.Equal(t, td.TimestampPoints[rowIdx].Content(), ts)

		sym, err := cols[symbolIndex+2].GetString()
		require.NoError(t, err)
		assert.Equal(t, td.SymbolPoints[rowIdx].Content(), sym)

		// Universal getter validation
		for i, c := range cols {
			if i == 1 { // skip $table

				continue
			}
			p := c.Get()
			switch p.Type() {
			case QueryResultBlob:
				assert.Equal(t, td.BlobPoints[rowIdx].Content(), p.Value())
			case QueryResultDouble:
				assert.Equal(t, td.DoublePoints[rowIdx].Content(), p.Value())
			case QueryResultInt64:
				assert.Equal(t, td.Int64Points[rowIdx].Content(), p.Value())
			case QueryResultString:
				v := p.Value().(string)
				exp1 := td.StringPoints[rowIdx].Content()
				exp2 := td.SymbolPoints[rowIdx].Content()
				assert.True(t, v == exp1 || v == exp2)
			case QueryResultTimestamp:
				assert.Equal(t, td.TimestampPoints[rowIdx].Content(), p.Value())
			case QueryResultNone:
				assert.Nil(t, p.Value())
			case QueryResultCount:
				// Count results should be int64
				assert.IsType(t, int64(0), p.Value())
			}
		}
	}
}

func TestQueryInvalidQueryReturnsError(t *testing.T) {
	handle := newTestHandle(t)

	_, err := handle.Query("select").Execute()
	assert.Error(t, err)
}

func TestQueryWrongTypeReturnsError(t *testing.T) {
	handle := newTestHandle(t)

	td := newTestTimeseriesAllColumns(t, handle, 1) // just 1 row needed
	query := fmt.Sprintf("select * from %s in range(1970, +10d)", td.Alias)
	result, err := handle.Query(query).Execute()
	require.NoError(t, err)
	defer handle.Release(unsafe.Pointer(result)) //nolint:gosec // Safe CGO conversion for QuasarDB API

	for _, row := range result.Rows() {
		cols := result.Columns(row)
		_, err := cols[blobIndex].GetDouble() // blob as double → error
		assert.Error(t, err)
	}
}

func TestQueryReturnsNoResults(t *testing.T) {
	handle := newTestHandle(t)

	td := newTestTimeseriesAllColumns(t, handle, 4)
	query := fmt.Sprintf("select * from %s in range(1971, +10d)", td.Alias)
	result, err := handle.Query(query).Execute()
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.ScannedPoints())
	assert.Equal(t, int64(0), result.RowCount())
}

func TestQueryCreateTableReturnsNil(t *testing.T) {
	handle := newTestHandle(t)

	alias := generateAlias(16)
	result, err := handle.Query(
		fmt.Sprintf("create table %s (id INT64, price DOUBLE)", alias),
	).Execute()
	require.NoError(t, err)
	assert.Nil(t, result)

	// cleanup
	_, _ = handle.Query(fmt.Sprintf("drop table %s", alias)).Execute()
}

func TestQueryDropTableReturnsNil(t *testing.T) {
	handle := newTestHandle(t)

	alias := generateAlias(16)
	_, _ = handle.Query(
		fmt.Sprintf("create table %s (id INT64, price DOUBLE)", alias),
	).Execute()

	result, err := handle.Query(
		fmt.Sprintf("drop table %s", alias),
	).Execute()
	require.NoError(t, err)
	assert.Nil(t, result)
}
