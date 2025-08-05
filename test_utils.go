package qdb

import (
	"errors"
	"fmt"
	"os"
	"slices"
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

// newTestHandle creates test cluster handle with automatic cleanup
// In: t *testing.T - test context
// Out: HandleType - connected handle with registered cleanup
// Ex: h := newTestHandle(t) → HandleType
func newTestHandle(t *testing.T) HandleType {
	t.Helper()

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := handle.Close()
		if err != nil && !errors.Is(err, ErrInvalidHandle) {
			t.Errorf("Failed to close handle: %v", err)
		}
	})

	return handle
}

// newTestDirectHandle returns a DirectHandle connected to the first
// cluster endpoint and registers clean-up callbacks for both the
// direct handle and the underlying HandleType.
func newTestDirectHandle(t *testing.T) DirectHandleType {
	t.Helper()

	handle := newTestHandle(t)
	cluster := handle.Cluster()

	endpoints, err := cluster.Endpoints()
	require.NoError(t, err)
	require.NotEmpty(t, endpoints)

	direct, err := handle.DirectConnect(endpoints[0].URI())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = direct.Close()
		// Note: handle.Close() is handled by newTestHandle() cleanup
	})

	return direct
}

// newTestWriterTable creates test table fixture
// In: t *testing.T - test context
// Out: WriterTable - table with all types
// Ex: wt := newTestWriterTable(t) → WriterTable
func newTestWriterTable(t *testing.T) WriterTable {
	t.Helper()

	tableName := generateDefaultAlias()
	columns := generateWriterColumnsOfAllTypes()

	writerTable, err := NewWriterTable(tableName, columns)
	require.NoError(t, err)
	require.NotNil(t, writerTable)

	return writerTable
}

// newTestWriter creates writer fixture with automatic cleanup
// In: t *testing.T - test context
// Out: Writer - default writer with registered cleanup
// Ex: w := newTestWriter(t) → Writer
func newTestWriter(t *testing.T) Writer {
	t.Helper()

	writer := NewWriterWithDefaultOptions()
	require.NotNil(t, writer)

	// Note: Writer does not have a Close() method, no cleanup needed

	return writer
}

// newTestNode creates a Node instance for testing purposes.
//
// Decision rationale:
//   - Centralizes Node creation to avoid duplicating URI handling across tests.
//   - Ensures consistent Node setup with the test handle's cluster URI.
//
// Key assumptions:
//   - handle is valid and connected to a running daemon.
//   - uri is a valid QuasarDB node URI.
//
// Performance trade-offs:
//   - Negligible; just wraps Node constructor.
//
// Usage example:
//
//	handle := newTestHandle(t)
//	node := newTestNode(handle, insecureURI)
func newTestNode(handle HandleType, uri string) *Node {
	return handle.Node(uri)
}

// newTestBlobWithContent creates a blob entry with content and automatic cleanup.
//
// Decision rationale:
//   - Centralizes blob creation logic used across cluster tests.
//   - Ensures consistent blob setup with content and automatic cleanup registration.
//
// Key assumptions:
//   - handle is valid and connected to a running daemon.
//   - Cleanup is automatically registered with t.Cleanup().
//
// Performance trade-offs:
//   - Negligible; just wraps blob creation and put operations.
//
// Usage example:
//
//	handle := newTestHandle(t)
//	blob, err := newTestBlobWithContent(t, handle, []byte("test content"))
//	// No need for defer blob.Remove() - cleanup is automatic
func newTestBlobWithContent(t *testing.T, handle HandleType, content []byte) (BlobEntry, error) {
	t.Helper()

	alias := generateAlias(16)
	blob := handle.Blob(alias)
	err := blob.Put(content, NeverExpires())
	if err != nil {
		return blob, err
	}

	t.Cleanup(func() {
		err := blob.Remove()
		if err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove blob: %v", err)
		}
	})

	return blob, nil
}

// newTestBlob creates a blob entry with automatic cleanup.
//
// Decision rationale:
//   - Provides a simple way to create blob entries without content for testing.
//   - Ensures automatic cleanup registration.
//
// Key assumptions:
//   - handle is valid and connected to a running daemon.
//   - Cleanup is automatically registered with t.Cleanup().
//
// Usage example:
//
//	handle := newTestHandle(t)
//	blob := newTestBlob(t, handle)
//	// No need for defer blob.Remove() - cleanup is automatic
func newTestBlob(t *testing.T, handle HandleType) BlobEntry {
	t.Helper()

	alias := generateAlias(16)
	blob := handle.Blob(alias)

	t.Cleanup(func() {
		err := blob.Remove()
		if err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove blob: %v", err)
		}
	})

	return blob
}

// newTestInteger creates an integer entry with automatic cleanup.
//
// Decision rationale:
//   - Provides a simple way to create integer entries for testing.
//   - Ensures automatic cleanup registration.
//
// Key assumptions:
//   - handle is valid and connected to a running daemon.
//   - Cleanup is automatically registered with t.Cleanup().
//
// Usage example:
//
//	handle := newTestHandle(t)
//	integer := newTestInteger(t, handle)
//	// No need for defer integer.Remove() - cleanup is automatic
func newTestInteger(t *testing.T, handle HandleType) IntegerEntry {
	t.Helper()

	alias := generateAlias(16)
	integer := handle.Integer(alias)

	t.Cleanup(func() {
		err := integer.Remove()
		if err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove integer: %v", err)
		}
	})

	return integer
}

// newTestWriterWithDefaultOptions creates a writer with default options and automatic cleanup.
//
// Decision rationale:
//   - Provides a simple way to create writers with default options for testing.
//   - Ensures automatic cleanup registration.
//
// Key assumptions:
//   - Cleanup is automatically registered with t.Cleanup().
//
// Usage example:
//
//	writer := newTestWriterWithDefaultOptions(t)
//	// No need for defer writer.Close() - cleanup is automatic
func newTestWriterWithDefaultOptions(t *testing.T) Writer {
	t.Helper()

	writer := NewWriterWithDefaultOptions()

	// Note: Writer does not have a Close() method, no cleanup needed

	return writer
}

// pushWriterTables writes tables to server
// In: t *testing.T - test context
//
//	handle HandleType - connection
//	tables []WriterTable - data to push
//
// Ex: pushWriterTables(t, h, tables)
func pushWriterTables(t *testing.T, handle HandleType, tables []WriterTable) {
	t.Helper()

	writer := newTestWriterWithDefaultOptions(t)

	for _, wt := range tables {
		require.NoError(t, writer.SetTable(wt))
	}

	require.NoError(t, writer.Push(handle))
}

// columnNamesFromWriterColumns extracts names
// In: cols []WriterColumn - columns
// Out: []string - column names
// Ex: columnNamesFromWriterColumns(cols) → ["a","b"]
func columnNamesFromWriterColumns(cols []WriterColumn) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.ColumnName
	}

	return names
}

// writerTableNames extracts table names
// In: tables []WriterTable - tables
// Out: []string - table names
// Ex: writerTableNames(tables) → ["t1","t2"]
func writerTableNames(tables []WriterTable) []string {
	names := make([]string, len(tables))
	for i, wt := range tables {
		names[i] = wt.GetName()
	}

	return names
}

// writerTableColumns gets table schema
// In: table WriterTable - table
// Out: []WriterColumn - columns
// Ex: writerTableColumns(t) → []WriterColumn
func writerTableColumns(table WriterTable) []WriterColumn {
	cols := make([]WriterColumn, len(table.columnInfoByOffset))
	copy(cols, table.columnInfoByOffset)

	return cols
}

// writerTablesColumns gets shared schema
// In: tables []WriterTable - tables
// Out: []WriterColumn - common schema
// Ex: writerTablesColumns(tables) → []WriterColumn
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

func genWriterDataInt64(t *rapid.T, rowCount int) *ColumnDataInt64 {
	values := make([]int64, rowCount)
	for i := range values {
		values[i] = rapid.Int64().Draw(t, "int64")
	}
	cd := NewColumnDataInt64(values)

	return &cd
}

func genWriterDataDouble(t *rapid.T, rowCount int) *ColumnDataDouble {
	values := make([]float64, rowCount)
	for i := range values {
		values[i] = rapid.Float64().Draw(t, "float64")
	}
	cd := NewColumnDataDouble(values)

	return &cd
}

func genWriterDataTimestamp(t *rapid.T, rowCount int) *ColumnDataTimestamp {
	values := make([]time.Time, rowCount)
	for i := range values {
		values[i] = genTime(t)
	}
	cd := NewColumnDataTimestamp(values)

	return &cd
}

func genWriterDataBlob(t *rapid.T, rowCount int) *ColumnDataBlob {
	values := make([][]byte, rowCount)
	for i := range values {
		values[i] = rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "blob")
	}
	cd := NewColumnDataBlob(values)

	return &cd
}

func genWriterDataString(t *rapid.T, rowCount int) *ColumnDataString {
	values := make([]string, rowCount)
	for i := range values {
		values[i] = rapid.StringN(1, 32, 64).Draw(t, "string")
	}
	cd := NewColumnDataString(values)

	return &cd
}

func genWriterData(t *rapid.T, rowCount int, ctype TsColumnType) ColumnData { //nolint:ireturn // Justified: Runtime type selection
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
	case TsColumnSymbol:

		return genWriterDataString(t, rowCount) // Symbols are handled same as strings for data generation
	case TsColumnUninitialized:
		panic(fmt.Sprintf("cannot generate data for uninitialized column type: %v", ctype))
	}
	panic(fmt.Sprintf("unknown column type: %v", ctype))
}

func genWriterDatas(t *rapid.T, rowCount int, columns []WriterColumn) []ColumnData {
	datas := make([]ColumnData, len(columns))
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
	columnType := rapid.SampledFrom(TsColumnTypes).Draw(t, "columnType")

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
func genReaderDataOfRowCountAndColumn(t *rapid.T, rowCount int, column ReaderColumn) ColumnData { //nolint:ireturn // Justified: Runtime type selection
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
	case TsValueNull:
		panic(fmt.Sprintf("Cannot generate reader data for null value type in column: %v", column))
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
func genReaderDataInt64(t *rapid.T, name string, rowCount int) *ColumnDataInt64 {
	values := make([]int64, rowCount)
	for i := range rowCount {
		values[i] = rapid.Int64().Draw(t, "int64")
	}

	ret := NewColumnDataInt64(values)

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
func genReaderDataDouble(t *rapid.T, name string, rowCount int) *ColumnDataDouble {
	values := make([]float64, rowCount)
	for i := range rowCount {
		values[i] = rapid.Float64().Draw(t, "float64")
	}

	ret := NewColumnDataDouble(values)

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
func genReaderDataTimestamp(t *rapid.T, name string, rowCount int) *ColumnDataTimestamp {
	values := make([]time.Time, rowCount)
	for i := range rowCount {
		values[i] = genTime(t)
	}

	ret := NewColumnDataTimestamp(values)

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
func genReaderDataBlob(t *rapid.T, name string, rowCount int) *ColumnDataBlob {
	values := make([][]byte, rowCount)
	for i := range rowCount {
		values[i] = rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "bytes")
	}

	ret := NewColumnDataBlob(values)

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
func genReaderDataString(t *rapid.T, name string, rowCount int) *ColumnDataString {
	values := make([]string, rowCount)
	for i := range rowCount {
		// Really random unicode, limit it to 32 characters and 64 bytes (unicode
		// can of course use more than 1 byte per character)
		values[i] = rapid.StringN(1, 32, 64).Draw(t, "string value")
	}

	ret := NewColumnDataString(values)

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

	data := make([]ColumnData, len(cols))
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

// generateTags returns n random tag strings produced via generateAlias.
//
// Decision rationale:
//   - Centralises tag creation so tests don’t rely on hard-coded values.
//   - Reuses generateAlias to guarantee tag-format consistency.
//
// Key assumptions:
//   - n > 0.
//   - generateAlias(16) yields sufficiently unique tags for test purposes.
//
// Usage example:
//
//	tags := generateTags(5)
func generateTags(n int) []string {
	if n <= 0 {
		panic("generateTags called with non-positive count")
	}
	ret := make([]string, n)
	for i := range ret {
		ret[i] = generateAlias(16)
	}

	return ret
}

func genWriterPushMode(t *rapid.T) WriterPushMode {
	return rapid.SampledFrom(writerPushModes).Draw(t, "writerPushMode")
}

// createTempFile writes content to a new file named
// <prefix>_<random>.tmp, registers automatic cleanup and returns the
// filename.
//
// Decision rationale:
//   - DRY helper for tests that need short-lived key / credential files.
//
// Usage example:
//
//	fname := createTempFile(t, "key", "secret")
func createTempFile(t *testing.T, prefix, content string) string {
	t.Helper()

	name := fmt.Sprintf("%s_%s.tmp", prefix, generateAlias(8))
	require.NoError(t, os.WriteFile(name, []byte(content), 0o600))
	t.Cleanup(func() { _ = os.Remove(name) })

	return name
}

// setupFindTestData creates three test entries (2 blobs, 1 integer) with
// predefined tag combinations and registers automatic cleanup via t.Cleanup.
//
// Returned slice layout: []string{blob1Alias, blob2Alias, integerAlias}
//
//nolint:gocritic // tooManyResultsChecker: test helper function, multiple return values needed for comprehensive setup
func setupFindTestData(
	t *testing.T,
	handle HandleType,
) (aliases []string,
	blob1, blob2 BlobEntry,
	integer IntegerEntry,
	tagAll, tagFirst, tagSecond, tagThird string,
) {
	t.Helper()

	tags := generateTags(4)
	tagAll, tagFirst = tags[0], tags[1]
	tagSecond, tagThird = tags[2], tags[3]

	// Generate unique aliases.
	aliasBlob1 := generateAlias(16)
	aliasBlob2 := generateAlias(16)
	aliasInteger := generateAlias(16)

	aliases = []string{aliasBlob1, aliasBlob2, aliasInteger}

	// Blob #1  – tags: all, first
	blob1 = handle.Blob(aliasBlob1)
	require.NoError(t, blob1.Put([]byte("asd"), NeverExpires()))
	require.NoError(t, blob1.AttachTag(tagAll))
	require.NoError(t, blob1.AttachTag(tagFirst))

	// Blob #2  – tags: all, second
	blob2 = handle.Blob(aliasBlob2)
	require.NoError(t, blob2.Put([]byte("asd"), NeverExpires()))
	require.NoError(t, blob2.AttachTag(tagAll))
	require.NoError(t, blob2.AttachTag(tagSecond))

	// Integer – tags: all, third
	integer = handle.Integer(aliasInteger)
	require.NoError(t, integer.Put(32, NeverExpires()))
	require.NoError(t, integer.AttachTag(tagAll))
	require.NoError(t, integer.AttachTag(tagThird))

	// Automatic cleanup.
	t.Cleanup(func() {
		if err := blob1.Remove(); err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove blob1: %v", err)
		}
		if err := blob2.Remove(); err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove blob2: %v", err)
		}
		if err := integer.Remove(); err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove integer: %v", err)
		}
	})

	return aliases, blob1, blob2, integer, tagAll, tagFirst, tagSecond, tagThird
}

var writerPushFlags = []WriterPushFlag{
	WriterPushFlagNone,
	WriterPushFlagWriteThrough,
	WriterPushFlagAsyncClientPush,
	WriterPushFlagWriteThrough | WriterPushFlagAsyncClientPush,
}

func genWriterPushFlag(t *rapid.T) WriterPushFlag {
	return rapid.SampledFrom(writerPushFlags).Draw(t, "writerPushFlag")
}

var writerDedupModes = []WriterDeduplicationMode{
	WriterDeduplicationModeDisabled,
	WriterDeduplicationModeDrop,
}

func genWriterDedupMode(t *rapid.T) WriterDeduplicationMode {
	return rapid.SampledFrom(writerDedupModes).Draw(t, "writerDedupMode")
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

	lhsIdx := slices.Clone(mergedIdx)
	rhsIdx := slices.Clone(rhs.idx)
	sort.Slice(lhsIdx, func(i, j int) bool { return lhsIdx[i].Before(lhsIdx[j]) })
	sort.Slice(rhsIdx, func(i, j int) bool { return rhsIdx[i].Before(rhsIdx[j]) })

	require.Equal(t, lhsIdx, rhsIdx, "index mismatch")
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

	expectedRows := 0
	for _, wt := range expected {
		rowCount := wt.RowCount()
		if rowCount < 0 || expectedRows > int(^uint(0)>>1)-rowCount {
			panic(fmt.Sprintf("integer overflow in row count calculation: expectedRows=%d, adding=%d", expectedRows, rowCount))
		}
		expectedRows += rowCount
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
	actualIdx := slices.Clone(rc.idx)
	sort.Slice(expectedIdx, func(i, j int) bool { return expectedIdx[i].Before(expectedIdx[j]) })
	sort.Slice(actualIdx, func(i, j int) bool { return actualIdx[i].Before(actualIdx[j]) })
	assert.Equal(t, expectedIdx, actualIdx, "timestamp index mismatch")

	// The reader doesn't guarantee any order, and what is the "index" for the Writer is just a column with
	// name "$timestamp" in the reader. Tables are not split out, and instead rely on the "$table" column name.
}

// -----------------------------------------------------------------
// Timeseries fixture used by query_test.go
// -----------------------------------------------------------------

// TestTimeseriesData bundles the alias and the sample points that
// newTestTimeseriesAllColumns inserts.
type TestTimeseriesData struct {
	Alias           string
	BlobPoints      []TsBlobPoint
	DoublePoints    []TsDoublePoint
	Int64Points     []TsInt64Point
	StringPoints    []TsStringPoint
	TimestampPoints []TsTimestampPoint
	SymbolPoints    []TsStringPoint
}

// newTestTimeseriesAllColumns creates a time-series that contains one
// column of every supported type, populates it with <count> rows of
// deterministic data, and registers automatic cleanup.
//
// Returned structure can be used by tests to verify query results.
func newTestTimeseriesAllColumns(t *testing.T, handle HandleType, count int64) TestTimeseriesData {
	t.Helper()

	alias := generateAlias(16)

	// Random (collision-free) column & symbol table names
	blobCol := generateColumnName()
	doubleCol := generateColumnName()
	int64Col := generateColumnName()
	stringCol := generateColumnName()
	timestampCol := generateColumnName()
	symbolCol := generateColumnName()
	symTable := generateAlias(16)

	cols := []TsColumnInfo{
		NewTsColumnInfo(blobCol, TsColumnBlob),
		NewTsColumnInfo(doubleCol, TsColumnDouble),
		NewTsColumnInfo(int64Col, TsColumnInt64),
		NewTsColumnInfo(stringCol, TsColumnString),
		NewTsColumnInfo(timestampCol, TsColumnTimestamp),
		NewSymbolColumnInfo(symbolCol, symTable),
	}

	ts := handle.Timeseries(alias)
	require.NoError(t, ts.Create(24*time.Hour, cols...))

	// Build sample data
	timestamps := make([]time.Time, count)
	blobPoints := make([]TsBlobPoint, count)
	doublePoints := make([]TsDoublePoint, count)
	int64Points := make([]TsInt64Point, count)
	stringPoints := make([]TsStringPoint, count)
	timestampPoints := make([]TsTimestampPoint, count)
	symbolPoints := make([]TsStringPoint, count)

	for i := range count {
		// Check for potential overflow before arithmetic
		if i >= int64(^uint(0)>>1)/10-1 {
			panic(fmt.Sprintf("integer overflow in timestamp calculation: i=%d", i))
		}
		tsVal := time.Unix((int64(i)+1)*10, 0)
		timestamps[i] = tsVal
		blobPoints[i] = NewTsBlobPoint(tsVal, []byte(fmt.Sprintf("content_%d", i)))
		doublePoints[i] = NewTsDoublePoint(tsVal, float64(i))
		int64Points[i] = NewTsInt64Point(tsVal, int64(i))
		stringPoints[i] = NewTsStringPoint(tsVal, fmt.Sprintf("content_%d", i))
		timestampPoints[i] = NewTsTimestampPoint(tsVal, tsVal)
		symbolPoints[i] = NewTsStringPoint(tsVal, fmt.Sprintf("content_%d", i))
	}

	require.NoError(t, ts.BlobColumn(blobCol).Insert(blobPoints...))
	require.NoError(t, ts.DoubleColumn(doubleCol).Insert(doublePoints...))
	require.NoError(t, ts.Int64Column(int64Col).Insert(int64Points...))
	require.NoError(t, ts.StringColumn(stringCol).Insert(stringPoints...))
	require.NoError(t, ts.TimestampColumn(timestampCol).Insert(timestampPoints...))
	require.NoError(t, ts.SymbolColumn(symbolCol, symTable).Insert(symbolPoints...))

	t.Cleanup(func() { _ = ts.Remove() })

	return TestTimeseriesData{
		Alias:           alias,
		BlobPoints:      blobPoints,
		DoublePoints:    doublePoints,
		Int64Points:     int64Points,
		StringPoints:    stringPoints,
		TimestampPoints: timestampPoints,
		SymbolPoints:    symbolPoints,
	}
}
