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

// writerTableNames returns the table names for the provided WriterTables.
func writerTableNames(tables []WriterTable) []string {
	names := make([]string, len(tables))
	for i, wt := range tables {
		names[i] = wt.GetName()
	}
	return names
}

// writerTableColumns returns the column schema for the provided WriterTable.
func writerTableColumns(table WriterTable) []WriterColumn {
	cols := make([]WriterColumn, len(table.columnInfoByOffset))
	copy(cols, table.columnInfoByOffset)
	return cols
}

// writerTablesColumns assumes all tables share the same schema and returns that
// schema. Panics if tables is empty.
func writerTablesColumns(tables []WriterTable) []WriterColumn {
	if len(tables) == 0 {
		panic("writerTablesColumns called with no tables")
	}
	return writerTableColumns(tables[0])
}

// genWriterColumnOfType generates a WriterColumn with the given type and a
// random ASCII name.
//
// Decision rationale:
//   - Provides granular control over the column type for schema-specific tests.
//   - Keeps names simple to avoid Unicode edge cases.
//
// Key assumptions:
//   - ctype is a valid TsColumnType.
//
// Performance trade-offs:
//   - Allocation of the name string only; negligible for property tests.
//
// Usage example:
//
//	col := genWriterColumnOfType(rt, TsColumnInt64)
func genWriterColumnOfType(t *rapid.T, ctype TsColumnType) WriterColumn {
	name := rapid.StringMatching(`[a-zA-Z]{8}`).Draw(t, "writerColumnName")
	return WriterColumn{ColumnName: name, ColumnType: ctype}
}

// genWriterColumn creates a WriterColumn with a random name and randomly
// selected type.
//
// Decision rationale:
//   - Used in property tests where any valid column type is acceptable.
//   - Reuses genWriterColumnOfType to centralize name generation logic.
//
// Performance trade-offs:
//   - Only draws from the generator; overhead is trivial.
//
// Usage example:
//
//	col := genWriterColumn(rt)
func genWriterColumn(t *rapid.T) WriterColumn {
	ctype := rapid.SampledFrom(columnTypes[:]).Draw(t, "writerColumnType")
	return genWriterColumnOfType(t, ctype)
}

// genWriterColumns returns between 1 and 8 randomly typed columns.
//
// Decision rationale:
//   - Exercises writer behavior with varying schema widths.
//   - Bound of eight keeps test cases manageable while covering most scenarios.
//
// Key assumptions:
//   - At least one column is always generated.
//
// Performance trade-offs:
//   - Linear in column count; negligible for ≤8 columns.
//
// Usage example:
//
//	cols := genWriterColumns(rt)
func genWriterColumns(t *rapid.T) []WriterColumn {
	genColumns := rapid.SliceOfN(rapid.Custom(genWriterColumn), 1, 8)

	return genColumns.Draw(t, "writerColumns")
}

// genWriterColumnsOfAllTypes returns one column for every supported type.
//
// Decision rationale:
//   - Useful when tests must cover all type-specific paths simultaneously.
//   - Names remain random to avoid clashes across repeated calls.
//
// Performance trade-offs:
//   - Allocates len(columnTypes) columns; still trivial in test context.
//
// Usage example:
//
//	cols := genWriterColumnsOfAllTypes(rt)
func genWriterColumnsOfAllTypes(t *rapid.T) []WriterColumn {
	cols := make([]WriterColumn, len(columnTypes))
	for i, ctype := range columnTypes {
		cols[i] = genWriterColumnOfType(t, ctype)
	}
	return cols
}

// genWriterColumnsOfType creates between 1 and 8 columns all sharing ctype.
//
// Decision rationale:
//   - Allows stressing multi-column writers while keeping value types uniform.
//
// Key assumptions:
//   - ctype is valid and supported by the writer.
//
// Performance trade-offs:
//   - O(n) allocation where n ∈ [1,8]; trivial for tests.
//
// Usage example:
//
//	cols := genWriterColumnsOfType(rt, TsColumnInt64)
func genWriterColumnsOfType(t *rapid.T, ctype TsColumnType) []WriterColumn {
	columnCount := rapid.IntRange(1, 8).Draw(t, "columnCount")
	cols := make([]WriterColumn, columnCount)

	for i := range columnCount {
		cols[i] = genWriterColumnOfType(t, ctype)
	}

	return cols
}

// genIndexAscending creates an increasing time index starting from a random time.
func genIndexAscending(t *rapid.T, rowCount int) []time.Time {
	start := genTime(t)
	stepNs := rapid.Int64Range(1, int64(time.Second)).Draw(t, "stepNs")
	idx := make([]time.Time, rowCount)
	for i := range rowCount {
		idx[i] = start.Add(time.Duration(stepNs * int64(i)))
	}
	return idx
}

// genWriterDataInt64 produces a WriterDataInt64 with rowCount random values.
//
// Decision rationale:
//   - Separates value generation from table creation helpers.
//   - Enables targeted testing of column type handling.
//
// Key assumptions:
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - O(rowCount) integer generation; trivial for typical test sizes.
func genWriterDataInt64(t *rapid.T, rowCount int) WriterData {
	values := make([]int64, rowCount)
	for i := range values {
		values[i] = rapid.Int64().Draw(t, "int64")
	}
	return NewWriterDataInt64(values)
}

// genWriterDataDouble returns a WriterDataDouble populated with random values.
//
// Key assumptions:
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - Generates one float64 per row; negligible in tests.
func genWriterDataDouble(t *rapid.T, rowCount int) WriterData {
	values := make([]float64, rowCount)
	for i := range values {
		values[i] = rapid.Float64().Draw(t, "float64")
	}
	return NewWriterDataDouble(values)
}

// genWriterDataTimestamp creates timestamp column data with rowCount entries.
//
// Decision rationale:
//   - Reuses genTime to ensure UTC timestamps covering broad ranges.
func genWriterDataTimestamp(t *rapid.T, rowCount int) WriterData {
	values := make([]time.Time, rowCount)
	for i := range values {
		values[i] = genTime(t)
	}
	return NewWriterDataTimestamp(values)
}

// genWriterDataBlob builds WriterDataBlob with random byte slices of length 1..64.
func genWriterDataBlob(t *rapid.T, rowCount int) WriterData {
	values := make([][]byte, rowCount)
	for i := range values {
		values[i] = rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "blob")
	}
	return NewWriterDataBlob(values)
}

// genWriterDataString returns a WriterDataString with random UTF-8 strings.
func genWriterDataString(t *rapid.T, rowCount int) WriterData {
	values := make([]string, rowCount)
	for i := range values {
		values[i] = rapid.StringN(1, 32, 64).Draw(t, "string")
	}
	return NewWriterDataString(values)
}

// genWriterData dispatches to the appropriate WriterData generator based on ctype.
//
// Key assumptions:
//   - rowCount ≥ 0.
//   - ctype matches one of the TsColumn* constants.
func genWriterData(t *rapid.T, rowCount int, ctype TsColumnType) WriterData {
	switch ctype {
	case TsColumnInt64:
		return genWriterDataInt64(t, rowCount)
	case TsColumnDouble:
		return genWriterDataDouble(t, rowCount)
	case TsColumnTimestamp:
		return genWriterDataTimestamp(t, rowCount)
	case TsColumnBlob:
		return genWriterDataBlob(t, rowCount)
	case TsColumnString:
		return genWriterDataString(t, rowCount)
	}
	panic(fmt.Sprintf("unknown column type: %v", ctype))
}

// genWriterDatas produces one WriterData per column using genWriterData.
//
// Key assumptions:
//   - len(columns) > 0.
//   - rowCount applies uniformly to all columns.
func genWriterDatas(t *rapid.T, rowCount int, columns []WriterColumn) []WriterData {
	datas := make([]WriterData, len(columns))
	for i, col := range columns {
		datas[i] = genWriterData(t, rowCount, col.ColumnType)
	}
	return datas
}

// genPopulatedTables creates tables in the QuasarDB instance and populates them
// with random data.
//
// Decision rationale:
//   - Provides end-to-end fixtures for writer and reader property tests.
//   - Delegates table creation to createTableOfWriterColumnsAndDefaultShardSize
//     for consistency with production code.
//
// Key assumptions:
//   - handle is valid and connected to a running daemon.
//
// Performance trade-offs:
//   - Table creation involves network I/O; keep tableCount small for speed.
func genPopulatedTables(t *rapid.T, handle HandleType) []WriterTable {
	tableCount := rapid.IntRange(1, 4).Draw(t, "tableCount")
	rowCount := rapid.IntRange(1, 64).Draw(t, "rowCount")

	columns := genWriterColumnsOfType(t, TsColumnInt64)
	idx := genIndexAscending(t, rowCount)
	datas := genWriterDatas(t, rowCount, columns)

	tables := make([]WriterTable, tableCount)

	for i := range tableCount {
		tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
		require.NoError(t, err)

		wt, err := NewWriterTable(tbl.alias, columns)
		require.NoError(t, err)
		wt.SetIndex(idx)
		require.NoError(t, wt.SetDatas(datas))

		tables[i] = wt
	}
	return tables
}

// genTime generates a random UTC time for property-based testing of time-related logic.
//
// Decision rationale:
//   - Samples both seconds and nanoseconds separately to cover edge cases across a broad temporal range.
//   - Ensures output is in UTC to avoid timezone-related variations.
//
// Key assumptions:
//   - Seconds are drawn uniformly from [0, 17_179_869_184), covering multiple centuries.
//   - Nanoseconds are drawn uniformly from [0, 1e9), covering full sub-second precision.
//
// Performance trade-offs:
//   - Negligible overhead relative to test suite runtime.
//
// Usage example:
//
//	t := rapid.MakeT()
//	ts := genTime(t) // ts is a randomized time.Time in UTC
func genTime(t *rapid.T) time.Time {
	sec := rapid.Int64Range(0, 8_147_483_646).Draw(t, "sec")
	nsec := rapid.Int64Range(0, 999_999_999).Draw(t, "nsec")
	return time.Unix(sec, nsec).UTC()
}

// genTimes generates a non-empty slice of UTC times for testing.
//
// Decision rationale:
//   - Delegates to genTime for each element, ensuring uniform random distribution.
//   - Uses rapid.SliceOf to vary slice length, exercising reader behavior on dynamic inputs.
//
// Key assumptions:
//   - The resulting slice has length ≥1.
//   - Each time value is independent and in UTC.
//
// Performance trade-offs:
//   - Leverages rapid's generator; overhead is minimal for typical test sizes.
//
// Usage example:
//
//	t := rapid.MakeT()
//	times := genTimes(t) // []time.Time, length ∈ [1, default upper bound]
func genTimes(t *rapid.T) []time.Time {
	genTimes := rapid.SliceOf(rapid.Custom(genTime))

	return genTimes.Draw(t, "times")
}

// genReaderColumn produces a random ReaderColumn with an 8-letter ASCII name and a random TsColumnType.
//
// Decision rationale:
//   - Uses fixed-length alphabetic names to simplify test scenarios and avoid unicode complexities.
//   - Samples from TsColumnTypes to cover all supported column types.
//
// Key assumptions:
//   - Name matches `[a-zA-Z]{8}`.
//   - TsColumnTypes slice includes all valid types for ReaderColumn.
//
// Performance trade-offs:
//   - Constant time generation; overhead negligible in test context.
//
// Usage example:
//
//	t := rapid.MakeT()
//	col := genReaderColumn(t) // ReaderColumn{Name: "AbCdEfGh", Type: TsValueInt64}
func genReaderColumn(t *rapid.T) ReaderColumn {
	// Column names are just a-zA-Z
	columnName := rapid.StringMatching(`[a-zA-Z]{8}`).Draw(t, "columnName")
	columnType := rapid.SampledFrom(TsColumnTypes[:]).Draw(t, "columnType")

	return ReaderColumn{
		columnName: columnName,
		columnType: columnType,
	}
}

// genReaderColumns generates between 1 and 8 ReaderColumn definitions for schema testing.
//
// Decision rationale:
//   - Varies column count to test dynamic schema handling.
//   - Upper bound of 8 balances complexity and coverage.
//
// Key assumptions:
//   - Minimum of 1 column avoids empty-schema edge cases.
//   - Downstream logic handles name uniqueness.
//
// Performance trade-offs:
//   - Linear in column count; trivial for test sizes.
//
// Usage example:
//
//	t := rapid.MakeT()
//	cols := genReaderColumns(t) // []ReaderColumn length ∈ [1,8]
func genReaderColumns(t *rapid.T) []ReaderColumn {
	// Between 1 and 8 columns
	genColumns := rapid.SliceOfN(rapid.Custom(genReaderColumn), 1, 8)

	return genColumns.Draw(t, "readerColumns")
}

// genReaderData generates a ReaderData instance for a random column with random row count.
//
// Decision rationale:
//   - Draws rowCount ∈ [1,1024] to simulate varying data sizes.
//   - Delegates to genReaderDataOfRowCount for type-specific value generation.
//
// Key assumptions:
//   - rowCount ≥ 1 ensures non-empty data sets.
//   - Column type selection occurs in downstream generation.
//
// Performance trade-offs:
//   - Generation cost is O(rowCount); acceptable in property tests.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rd := genReaderData(t) // ReaderData with random schema and data
func genReaderData(t *rapid.T) ReaderData {
	rowCount := rapid.IntRange(1, 1024).Draw(t, "rowCount")
	return genReaderDataOfRowCount(t, rowCount)
}

// genReaderDataOfRowCount generates ReaderData for a single randomly chosen column and given rowCount.
//
// Decision rationale:
//   - Separates rowCount control from schema generation for flexible tests.
//   - Randomly selects column type to cover all data paths.
//
// Key assumptions:
//   - rowCount ≥ 1.
//   - genReaderColumn yields valid ReaderColumn metadata.
//
// Performance trade-offs:
//   - One slice creation per value; linear in rowCount.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rd := genReaderDataOfRowCount(t, 100) // 100 rows of random data for one column
func genReaderDataOfRowCount(t *rapid.T, rowCount int) ReaderData {
	column := rapid.Custom(genReaderColumn).Draw(t, "columnType")

	return genReaderDataOfRowCountAndColumn(t, rowCount, column)
}

// genReaderDataOfRowCountAndColumn generates ReaderData matching the provided schema for a fixed row count.
//
// Decision rationale:
//   - Routes to type-specific generators based on columnType.AsValueType().
//   - Ensures data aligns with ReaderColumn metadata for schema consistency.
//
// Key assumptions:
//   - rowCount ≥ 0.
//   - column.columnType.AsValueType() covers all TsValue* cases.
//   - Panics on invalid type to signal incorrect test configuration.
//
// Performance trade-offs:
//   - Single pass through rowCount and type dispatch; linear in rowCount.
//
// Usage example:
//
//	t := rapid.MakeT()
//	col := genReaderColumn(t)
//	rd := genReaderDataOfRowCountAndColumn(t, 50, col)
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

// genReaderDataInt64 generates a ReaderDataInt64 instance with random int64 values.
//
// Decision rationale:
//   - Uses rapid.Int64() for full-range integer testing.
//   - Wraps values in ReaderDataInt64 guaranteeing correct API usage.
//
// Key assumptions:
//   - name is a valid column identifier.
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - O(rowCount) time and memory; acceptable in test suites.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rdi := genReaderDataInt64(t, "col_int64", 10) // 10 random int64s
func genReaderDataInt64(t *rapid.T, name string, rowCount int) *ReaderDataInt64 {
	values := make([]int64, rowCount)
	for i := range rowCount {
		values[i] = rapid.Int64().Draw(t, "int64")
	}

	ret := newReaderDataInt64(name, values)
	return &ret
}

// genReaderDataDouble generates a ReaderDataDouble instance with random float64 values.
//
// Decision rationale:
//   - Uses rapid.Float64() to cover special float values (NaN, ±Inf) and standard range.
//   - Encapsulates values in ReaderDataDouble for type safety.
//
// Key assumptions:
//   - name is a valid column identifier.
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - O(rowCount) generation cost; negligible in test contexts.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rdd := genReaderDataDouble(t, "col_double", 5) // 5 random float64s
func genReaderDataDouble(t *rapid.T, name string, rowCount int) *ReaderDataDouble {
	values := make([]float64, rowCount)
	for i := range rowCount {
		values[i] = rapid.Float64().Draw(t, "float64")
	}

	ret := newReaderDataDouble(name, values)
	return &ret
}

// genReaderDataTimestamp generates a ReaderDataTimestamp instance with random UTC time values.
//
// Decision rationale:
//   - Reuses genTime to produce high-precision timestamps across broad ranges.
//   - Encapsulates values in ReaderDataTimestamp for API conformity.
//
// Key assumptions:
//   - name is a valid column identifier.
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - O(rowCount) cost driven by genTime complexity.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rdt := genReaderDataTimestamp(t, "ts_col", 3) // 3 random timestamps
func genReaderDataTimestamp(t *rapid.T, name string, rowCount int) *ReaderDataTimestamp {
	values := make([]time.Time, rowCount)
	for i := range rowCount {
		values[i] = genTime(t)
	}

	ret := newReaderDataTimestamp(name, values)
	return &ret
}

// genReaderDataBlob generates a ReaderDataBlob instance with random byte slices.
//
// Decision rationale:
//   - Uses rapid.SliceOfN(rapid.Byte(),1,64) to create blobs varying from 1 to 64 bytes.
//   - Tests binary data paths with realistic size distributions.
//
// Key assumptions:
//   - name is a valid column identifier.
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - O(total_bytes) generation cost, suitable for unit tests.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rdb := genReaderDataBlob(t, "blob_col", 4) // 4 random blobs
func genReaderDataBlob(t *rapid.T, name string, rowCount int) *ReaderDataBlob {
	values := make([][]byte, rowCount)
	for i := range rowCount {
		values[i] = rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "bytes")
	}

	ret := newReaderDataBlob(name, values)
	return &ret
}

// genReaderDataString generates a ReaderDataString instance with random Unicode strings.
//
// Decision rationale:
//   - Uses rapid.StringN(1,32,64) to enforce ≤32 characters and ≤64 bytes per string.
//   - Covers multibyte UTF-8 scenarios in tests.
//
// Key assumptions:
//   - name is a valid column identifier.
//   - rowCount ≥ 0.
//
// Performance trade-offs:
//   - O(total_chars) cost proportional to string lengths.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rds := genReaderDataString(t, "str_col", 6) // 6 random strings
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

// genReaderChunkOfSchema generates a ReaderChunk for a fixed schema and random row count.
//
// Decision rationale:
//   - Draws rowCount ∈ [1,1024] and index times via genTime for realistic row positions.
//   - Constructs column data arrays matching schema to validate reader chunk assembly.
//
// Key assumptions:
//   - cols slice length ≥ 1 defines the schema.
//   - NewReaderChunk enforces length and type consistency.
//
// Performance trade-offs:
//   - O(rowCount * numColumns) data generation; reasonable for property tests.
//
// Usage example:
//
//	t := rapid.MakeT()
//	schema := genReaderColumns(t)
//	rc := genReaderChunkOfSchema(t, schema)
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

// genReaderChunk generates a ReaderChunk with randomized schema and data.
//
// Decision rationale:
//   - Combines schema generation (genReaderColumns) and chunk construction for end-to-end tests.
//   - Ensures reader logic handles variable schemas and data sizes in one flow.
//
// Key assumptions:
//   - At least one column and one row are generated per chunk.
//   - Schema and data lengths are consistent.
//
// Performance trade-offs:
//   - Aggregate cost of column and row generation; acceptable for unit/property tests.
//
// Usage example:
//
//	t := rapid.MakeT()
//	rc := genReaderChunk(t) // ReaderChunk with random schema and rows
func genReaderChunk(t *rapid.T) ReaderChunk {

	columns := genReaderColumns(t)

	return genReaderChunkOfSchema(t, columns)
}

// genReaderChunks generates a slice of ReaderChunks sharing a consistent schema.
//
// Decision rationale:
//   - Validates bulk reader requirements that all chunks in a batch conform to one schema.
//   - Varies the number of chunks between 1 and 8 to simulate realistic batched reads.
//
// Key assumptions:
//   - All returned ReaderChunk elements use identical column schemas.
//   - Row counts across chunks may differ for coverage.
//
// Performance trade-offs:
//   - Overhead proportional to total rows across chunks; suitable for test suites.
//
// Usage example:
//
//	t := rapid.MakeT()
//	chunks := genReaderChunks(t) // []ReaderChunk length ∈ [1,8]
func genReaderChunks(t *rapid.T) []ReaderChunk {

	cols := genReaderColumns(t)

	genChunk := rapid.Custom(func(t *rapid.T) ReaderChunk {
		return genReaderChunkOfSchema(t, cols)
	})

	genChunks := rapid.SliceOfN(genChunk, 1, 8)
	return genChunks.Draw(t, "readerChunks")
}

var writerPushModes = []WriterPushMode{
	WriterPushModeTransactional,
	WriterPushModeFast,
	WriterPushModeAsync,
}

func genWriterPushMode(t *rapid.T) WriterPushMode {
	return rapid.SampledFrom(writerPushModes[:]).Draw(t, "writerPushMode")
}

var writerPushFlags = []WriterPushFlag{
	WriterPushFlagNone,
	WriterPushFlagWriteThrough,
	WriterPushFlagAsyncClientPush,
	WriterPushFlagWriteThrough | WriterPushFlagAsyncClientPush,
}

func genWriterPushFlag(t *rapid.T) WriterPushFlag {
	return rapid.SampledFrom(writerPushFlags[:]).Draw(t, "writerPushFlag")
}

var writerDedupModes = []WriterDeduplicationMode{
	WriterDeduplicationModeDisabled,
	WriterDeduplicationModeDrop,
}

func genWriterDedupMode(t *rapid.T) WriterDeduplicationMode {
	return rapid.SampledFrom(writerDedupModes[:]).Draw(t, "writerDedupMode")
}

// genWriterOptions constructs a WriterOptions value using random combinations
// of push mode, flags and deduplication settings.
//
// Decision rationale:
//   - Exercises the full WriterOptions API in property tests by varying each
//     independent parameter.
//   - Clears dropDuplicateColumns so callers can supply their own columns when
//     needed.
//
// Key assumptions:
//   - genWriterPushMode, genWriterPushFlag and genWriterDedupMode return valid
//     enum values.
//   - The resulting options must satisfy opts.IsValid(); generation panics if it
//     does not.
//
// Performance trade-offs:
//   - Allocation and assignments only; overhead is negligible relative to test
//     execution time.
//
// Usage example:
//
//	rt := rapid.MakeT()
//	opts := genWriterOptions(rt)
func genWriterOptions(t *rapid.T) WriterOptions {
	opts := NewWriterOptions()
	opts.pushMode = genWriterPushMode(t)
	opts.pushFlags = genWriterPushFlag(t)
	opts = opts.WithDeduplicationMode(genWriterDedupMode(t))
	opts.dropDuplicateColumns = nil

	if !opts.IsValid() {
		panic("genWriterOptions produced invalid options")
	}

	return opts
}

// testHelper defines the minimal testing interface implemented by both *testing.T
// and rapid's property-testing T.
//
// Decision rationale:
//   - Allows helper assertions to accept either testing framework without
//     duplication.
//   - Exposes Helper() so error lines reference the caller instead of the helper.
//
// Key assumptions:
//   - Any implementation must satisfy require.TestingT (typically *testing.TB).
//   - Helper() marks the function as a helper for better test diagnostics.
//
// Performance trade-offs:
//   - None; interface dispatch cost is negligible in unit tests.
type testHelper interface {
	require.TestingT
	Helper()
}

// assertReaderChunksEqualChunk verifies that merging lhs chunks produces rhs.
//
// Decision rationale:
//   - Simplifies equality checks when mergeReaderChunks is expected to behave
//     identically to manual concatenation.
//
// Key assumptions:
//   - lhs is non-empty and all chunks share one schema.
//   - rhs uses the same schema as lhs and contains the combined rows.
//
// Performance trade-offs:
//   - Indexes are copied once for sorting; cost is O(totalRows).
//
// Usage example:
//
//	assertReaderChunksEqualChunk(rt, left, merged)
func assertReaderChunksEqualChunk(t testHelper, lhs []ReaderChunk, rhs ReaderChunk) {
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

	require.Equal(t, lhsIdx, rhsIdx, "index mismatch")
}

// writerDataToReaderData converts a WriterData instance to the corresponding
// ReaderData implementation.
//
// Decision rationale:
//   - Allows test helpers to reuse writer generators when validating reader
//     results.
//   - Switches on wd.valueType() to avoid fragile type assertions.
//
// Key assumptions:
//   - wd was produced by NewWriterData* helpers and thus implements valueType().
//
// Performance trade-offs:
//   - Only wraps the existing slice; no additional allocations occur.
func writerDataToReaderData(name string, wd WriterData) ReaderData {
	switch wd.valueType() {
	case TsValueInt64:
		return writerDataInt64ToReaderDataInt64(name, *GetWriterDataInt64Unsafe(wd))
	case TsValueDouble:
		return writerDataDoubleToReaderDataDouble(name, *GetWriterDataDoubleUnsafe(wd))
	case TsValueTimestamp:
		return writerDataTimeToReaderDataTimestamp(name, *GetWriterDataTimestampUnsafe(wd))
	case TsValueBlob:
		return writerDataBlobToReaderDataBlob(name, *GetWriterDataBlobUnsafe(wd))
	case TsValueString:
		return writerDataStringToReaderDataString(name, *GetWriterDataStringUnsafe(wd))
	}

	panic(fmt.Sprintf("unrecognized value type: %v", wd.valueType()))
}

func writerDataInt64ToReaderDataInt64(name string, wd WriterDataInt64) *ReaderDataInt64 {
	ret := newReaderDataInt64(name, wd.xs)
	return &ret
}

// writerDataDoubleToReaderDataDouble converts WriterDataDouble to ReaderDataDouble.
func writerDataDoubleToReaderDataDouble(name string, wd WriterDataDouble) *ReaderDataDouble {
	ret := newReaderDataDouble(name, wd.xs)
	return &ret
}

// writerDataTimeToReaderDataTimestamp converts WriterDataTimestamp to
// ReaderDataTimestamp.
//
// Decision rationale:
//   - Relies on QdbTimespecSliceToTime for lossless conversion.
//
// Key assumptions:
//   - wd.xs uses UTC-based timespec values.
func writerDataTimeToReaderDataTimestamp(name string, wd WriterDataTimestamp) *ReaderDataTimestamp {
	vals := QdbTimespecSliceToTime(wd.xs)
	ret := newReaderDataTimestamp(name, vals)
	return &ret
}

// writerDataBlobToReaderDataBlob converts WriterDataBlob to ReaderDataBlob.
// No deep copy is required as both hold Go-managed slices.
func writerDataBlobToReaderDataBlob(name string, wd WriterDataBlob) *ReaderDataBlob {
	ret := newReaderDataBlob(name, wd.xs)
	return &ret
}

// writerDataStringToReaderDataString converts WriterDataString to ReaderDataString.
// Strings are copied by slice header only; underlying data is shared.
func writerDataStringToReaderDataString(name string, wd WriterDataString) *ReaderDataString {
	ret := newReaderDataString(name, wd.xs)
	return &ret
}

// Converts a writerColumn to a readerColumn
func writerColumnToReaderColumn(wc WriterColumn) ReaderColumn {
	ret, err := NewReaderColumn(wc.ColumnName, wc.ColumnType)

	if err != nil {
		panic(fmt.Sprintf("unable to convert writer column to reader column: %v", err))
	}

	return ret
}

// Converts a WriterTable to a ReaderChunk
func writerTableToReaderChunk(wt WriterTable) ReaderChunk {

	idx := wt.GetIndex()

	// Pre-allocate all output data
	data := make([]ReaderData, len(wt.data))

	// pseudo-code:
	// 1. for each WriterData in wt
	//   - look up column info of column based on offset
	//   - get column name from column info

	for idx := range len(wt.data) {
		colName := wt.columnInfoByOffset[idx].ColumnName
		data[idx] = writerDataToReaderData(colName, wt.data[idx])
	}

	columns := make([]ReaderColumn, len(wt.columnInfoByOffset))
	for i, wc := range wt.columnInfoByOffset {
		columns[i] = writerColumnToReaderColumn(wc)
	}

	ret, err := NewReaderChunk(columns, idx, data)

	if err != nil {
		panic(fmt.Sprintf("unable to convert writer table to reader chunk: %v", err))
	}

	return ret

}

// Converts the writer table data to reader chunks, groups all reader chunks
// per table.
func writerTablesToReaderChunks(xs []WriterTable) map[string]ReaderChunk {
	ret := make(map[string]ReaderChunk, len(xs))

	for _, wt := range xs {
		rc := writerTableToReaderChunk(wt)
		ret[wt.GetName()] = rc
	}

	return ret
}

// assertWriterTablesEqualReaderChunks checks that rc contains exactly the rows
// written in expected tables.
//
// Decision rationale:
//   - Consolidates row-count validation across tests.
//   - Serves as the first step toward full data comparison.
//
// Key assumptions:
//   - expected tables were pushed in the same order as names.
//   - rc was returned by FetchAll for those table names.
//
// Usage example:
//
//	assertWriterTablesEqualReaderChunks(rt, tables, names, chunk)
func assertWriterTablesEqualReaderChunks(t testHelper, expected []WriterTable, names []string, rc ReaderChunk) {
	t.Helper()

	var expectedRows int = 0
	for _, wt := range expected {
		expectedRows += wt.RowCount()
	}

	assert.Equal(t, expectedRows, rc.RowCount(), "row count mismatch")

	for i, col := range rc.data {
		assert.Equal(t, rc.RowCount(), col.Length(), "column %d length mismatch", i)
	}

	// Compare timestamp indexes across all expected tables (order-agnostic).
	expectedIdx := make([]time.Time, 0, expectedRows)
	for _, wt := range expected {
		expectedIdx = append(expectedIdx, wt.GetIndex()...)
	}
	// Copy and sort both slices before comparing
	actualIdx := append([]time.Time(nil), rc.idx...)
	sort.Slice(expectedIdx, func(i, j int) bool { return expectedIdx[i].Before(expectedIdx[j]) })
	sort.Slice(actualIdx,   func(i, j int) bool { return actualIdx[i].Before(actualIdx[j]) })
	assert.Equal(t, expectedIdx, actualIdx, "timestamp index mismatch")

	// The reader doesn't guarantee any order, and what is the "index" for the Writer is just a column with
	// name "$timestamp" in the reader. Tables are not split out, and instead rely on the "$table" column name.
}
