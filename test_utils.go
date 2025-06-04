package qdb

import (
	"fmt"
	"sort"
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
func assertWriterTablesEqualReaderChunks(t *testing.T, expected []WriterTable, names []string, rc ReaderChunk) {
	t.Helper()

	// TODO: implement
}

// genTime generates a random UTC time as used in time_test.go.
func genTime(t *rapid.T) time.Time {
	sec := rapid.Int64Range(0, 17_179_869_184).Draw(t, "sec")
	nsec := rapid.Int64Range(0, 999_999_999).Draw(t, "nsec")
	return time.Unix(sec, nsec).UTC()
}

// genReaderChunks returns between 1 and 8 ReaderChunks that all share the same
// schema.
func genTimes(t *rapid.T) []time.Time {
	genTimes := rapid.SliceOf(rapid.Custom(genTime))

	return genTimes.Draw(t, "times")
}

// Generator for `ReaderColumn`
func genReaderColumn(t *rapid.T) ReaderColumn {
	// Column names are just a-zA-Z
	columnName := rapid.StringMatching(`[a-zA-Z]{8}`).Draw(t, "columnName")
	columnType := rapid.SampledFrom(TsColumnTypes[:]).Draw(t, "columnType")

	return ReaderColumn{
		columnName: columnName,
		columnType: columnType,
	}
}

func genReaderColumns(t *rapid.T) []ReaderColumn {
	// Between 1 and 8 columns
	genColumns := rapid.SliceOfN(rapid.Custom(genReaderColumn), 1, 8)

	return genColumns.Draw(t, "readerColumns")
}

func genReaderData(t *rapid.T) ReaderData {
	rowCount := rapid.IntRange(1, 1024).Draw(t, "rowCount")
	return genReaderDataOfRowCount(t, rowCount)
}

func genReaderDataOfRowCount(t *rapid.T, rowCount int) ReaderData {
	column := rapid.Custom(genReaderColumn).Draw(t, "columnType")

	return genReaderDataOfRowCountAndColumn(t, rowCount, column)
}

func genReaderDataOfRowCountAndColumn(t *rapid.T, rowCount int, column ReaderColumn) ReaderData {
	switch column.columnType.AsValueType() {
	case TsValueInt64:
		return genReaderDataInt64(t, column.Name(), rowCount)
	case TsValueDouble:
		return genReaderDataDouble(t, column.Name(), rowCount)
	case TsValueTimestamp:
		return genReaderDataTimestamp(t, column.Name(), rowCount)
	case TsValueBlob:
		return genReaderDataBlob(t, column.Name(), rowCount)
	case TsValueString:
		return genReaderDataString(t, column.Name(), rowCount)
	}

	panic(fmt.Sprintf("Invalid column type for column: %v", column))
}

func genReaderDataInt64(t *rapid.T, name string, rowCount int) *ReaderDataInt64 {
	values := make([]int64, rowCount)
	for i := range rowCount {
		values[i] = rapid.Int64().Draw(t, "int64")
	}

	ret := newReaderDataInt64(name, values)
	return &ret
}

func genReaderDataDouble(t *rapid.T, name string, rowCount int) *ReaderDataDouble {
	values := make([]float64, rowCount)
	for i := range rowCount {
		values[i] = rapid.Float64().Draw(t, "float64")
	}

	ret := newReaderDataDouble(name, values)
	return &ret
}

func genReaderDataTimestamp(t *rapid.T, name string, rowCount int) *ReaderDataTimestamp {
	values := make([]time.Time, rowCount)
	for i := range rowCount {
		values[i] = genTime(t)
	}

	ret := newReaderDataTimestamp(name, values)
	return &ret
}

func genReaderDataBlob(t *rapid.T, name string, rowCount int) *ReaderDataBlob {
	values := make([][]byte, rowCount)
	for i := range rowCount {
		values[i] = rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "bytes")
	}

	ret := newReaderDataBlob(name, values)
	return &ret
}

func genReaderDataString(t *rapid.T, name string, rowCount int) *ReaderDataString {
	values := make([]string, rowCount)
	for i := range rowCount {
		// Really random unicode, limit it to 32 characters and 64 bytes (unicode
		// can of course use more than 1 byte per character)
		values[i] = rapid.StringN(1, 32, 64).Draw(t, "string value")
	}

	ret := newReaderDataString(name, values)
	return &ret
}

func genReaderChunkOfSchema(t *rapid.T, cols []ReaderColumn) ReaderChunk {

	rowCount := rapid.IntRange(1, 1024).Draw(t, "rowCount")

	idx := make([]time.Time, rowCount)
	for i := range rowCount {
		idx[i] = genTime(t)
	}

	data := make([]ReaderData, len(cols))
	for i, col := range cols {
		data[i] = genReaderDataOfRowCountAndColumn(t, rowCount, col)
	}

	ret, err := NewReaderChunk(
		cols,
		idx,
		data)

	if err != nil {
		panic(err)
	}

	return ret
}

// genReaderChunk constructs a ReaderChunk with random columns.
// The chunk will contain between 1 and 1024 rows and up to 8 columns.
func genReaderChunk(t *rapid.T) ReaderChunk {

	columns := genReaderColumns(t)

	return genReaderChunkOfSchema(t, columns)
}

// genReaderChunks returns between 1 and 8 ReaderChunks that all share the same
// schema. This is important because the bulk reader always requires and ensures
// that all chunks within a single operation share the same schema, and this
// makes the data realistic.
func genReaderChunks(t *rapid.T) []ReaderChunk {

	cols := genReaderColumns(t)

	genChunk := rapid.Custom(func(t *rapid.T) ReaderChunk {
		return genReaderChunkOfSchema(t, cols)
	})

	genChunks := rapid.SliceOfN(genChunk, 1, 8)
	return genChunks.Draw(t, "readerChunks")
}

func assertReaderChunksEqualChunk(t *testing.T, lhs []ReaderChunk, rhs ReaderChunk) {
	t.Helper()

	// Ensure lhs contains data to compare.
	require.NotEmpty(t, lhs, "lhs must contain at least one chunk")

	baseCols := lhs[0].columnInfoByOffset

	// All lhs chunks must share the same schema while counting rows.
	totalRows := 0
	for i, c := range lhs {
		require.Equal(t, baseCols, c.columnInfoByOffset, "lhs[%d] schema mismatch", i)
		totalRows += c.RowCount()
	}

	require.Equal(t, baseCols, rhs.columnInfoByOffset, "rhs schema mismatch")
	require.Equal(t, totalRows, len(rhs.idx), "row count mismatch")

	// Build a merged index from lhs and compare after sorting.
	mergedIdx := make([]time.Time, 0, totalRows)
	for _, c := range lhs {
		mergedIdx = append(mergedIdx, c.idx...)
	}

	lhsIdx := append([]time.Time(nil), mergedIdx...)
	rhsIdx := append([]time.Time(nil), rhs.idx...)
	sort.Slice(lhsIdx, func(i, j int) bool { return lhsIdx[i].Before(lhsIdx[j]) })
	sort.Slice(rhsIdx, func(i, j int) bool { return rhsIdx[i].Before(rhsIdx[j]) })

	assert.Equal(t, lhsIdx, rhsIdx, "index mismatch")
}
