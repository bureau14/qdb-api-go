package qdb

/*
#include <qdb/client.h>
#include <qdb/query.h>

// Cell setters for hand-built query results. The fixture writes the payload
// union through the C compiler and the converter reads it back through Go
// unsafe loads, so every converter test also checks the union layout that
// Go cannot see (cgo flattens the union to a byte array).
static inline void set_point_type(qdb_point_result_t * p, qdb_query_result_value_type_t t)
{
	p->type = t;
}

static inline void set_point_int64(qdb_point_result_t * p, qdb_int_t v)
{
	p->type = qdb_query_result_int64;
	p->payload.int64_.value = v;
}

static inline void set_point_double(qdb_point_result_t * p, double v)
{
	p->type = qdb_query_result_double;
	p->payload.double_.value = v;
}

static inline void set_point_count(qdb_point_result_t * p, qdb_size_t v)
{
	p->type = qdb_query_result_count;
	p->payload.count.value = v;
}

static inline void set_point_timestamp(qdb_point_result_t * p, qdb_time_t sec, qdb_time_t nsec)
{
	p->type = qdb_query_result_timestamp;
	p->payload.timestamp.value.tv_sec = sec;
	p->payload.timestamp.value.tv_nsec = nsec;
}

static inline void set_point_string(qdb_point_result_t * p, const char * c, qdb_size_t n)
{
	p->type = qdb_query_result_string;
	p->payload.string.content = c;
	p->payload.string.content_length = n;
}

static inline void set_point_blob(qdb_point_result_t * p, const void * c, qdb_size_t n)
{
	p->type = qdb_query_result_blob;
	p->payload.blob.content = c;
	p->payload.blob.content_length = n;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"testing"
	"time"
	"unsafe"

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
// In: t testHelper - test context (works with *testing.T and *rapid.T)
// Out: HandleType - connected handle with registered cleanup
// Ex: h := newTestHandle(t) → HandleType
func newTestHandle(t testHelper) HandleType {
	t.Helper()

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.NoError(t, err)

	// Use type assertion to access Cleanup method
	switch v := t.(type) {
	case *testing.T:
		v.Cleanup(func() {
			err := handle.Close()
			if err != nil && !errors.Is(err, ErrInvalidHandle) {
				v.Errorf("Failed to close handle: %v", err)
			}
		})
	case *rapid.T:
		v.Cleanup(func() {
			err := handle.Close()
			if err != nil && !errors.Is(err, ErrInvalidHandle) {
				v.Errorf("Failed to close handle: %v", err)
			}
		})
	default:
		// For other test helpers, we need to close manually
		// This is a fallback - ideally all test contexts should support Cleanup
		t.Logf("Warning: test context type %T does not support Cleanup, handle may leak", t)
	}

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

// genWriterDataSymbol generates symbol column data. Unlike string columns,
// symbol values become symtable entries and the server rejects arbitrary
// bytes (e.g. NUL), so values are restricted to alphanumeric strings.
func genWriterDataSymbol(t *rapid.T, rowCount int) *ColumnDataString {
	values := make([]string, rowCount)
	for i := range values {
		values[i] = rapid.StringMatching(`[a-zA-Z0-9]{1,16}`).Draw(t, "symbol")
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

		return genWriterDataSymbol(t, rowCount) // Symbol values must be valid symtable entries
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
	return genPopulatedTablesOfType(t, handle, TsColumnInt64)
}

// genPopulatedTablesOfType is genPopulatedTables with all value columns
// sharing ctype (e.g. TsColumnSymbol to exercise symbol pushes).
func genPopulatedTablesOfType(t *rapid.T, handle HandleType, ctype TsColumnType) []WriterTable {
	tableCount := rapid.IntRange(1, 4).Draw(t, "tableCount")
	rowCount := rapid.IntRange(1, 64).Draw(t, "rowCount")

	columns := genWriterColumnsOfType(t, ctype)
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
		err := blob1.Remove()
		if err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove blob1: %v", err)
		}
		err = blob2.Remove()
		if err != nil && !errors.Is(err, ErrAliasNotFound) {
			t.Errorf("Failed to remove blob2: %v", err)
		}
		err = integer.Remove()
		if err != nil && !errors.Is(err, ErrAliasNotFound) {
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
	Logf(format string, args ...interface{})
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

// TestTimeseriesData bundles the alias, the sample points that the
// all-columns fixture inserts, and one validity mask per column. Point
// slices always hold dense values; a cleared mask bit means the cell was
// pushed as the QDB_IS_NULL_* sentinel and the query returns it as none.
type TestTimeseriesData struct {
	Alias           string
	BlobPoints      []TsBlobPoint
	DoublePoints    []TsDoublePoint
	Int64Points     []TsInt64Point
	StringPoints    []TsStringPoint
	TimestampPoints []TsTimestampPoint
	SymbolPoints    []TsStringPoint
	BlobValid       []bool
	DoubleValid     []bool
	Int64Valid      []bool
	StringValid     []bool
	TimestampValid  []bool
	SymbolValid     []bool
}

// allColumnsSchema is the column set of the all-columns fixture: one
// column of every type, with random names so parallel tests never collide.
type allColumnsSchema struct {
	blob, double, int64, str, timestamp, symbol string
}

// newAllColumnsSchema creates the table and registers its removal.
func newAllColumnsSchema(t *testing.T, handle HandleType, alias string) allColumnsSchema {
	t.Helper()

	s := allColumnsSchema{
		blob:      generateColumnName(),
		double:    generateColumnName(),
		int64:     generateColumnName(),
		str:       generateColumnName(),
		timestamp: generateColumnName(),
		symbol:    generateColumnName(),
	}
	cols := []TsColumnInfo{
		NewTsColumnInfo(s.blob, TsColumnBlob),
		NewTsColumnInfo(s.double, TsColumnDouble),
		NewTsColumnInfo(s.int64, TsColumnInt64),
		NewTsColumnInfo(s.str, TsColumnString),
		NewTsColumnInfo(s.timestamp, TsColumnTimestamp),
		NewSymbolColumnInfo(s.symbol, generateAlias(16)),
	}

	ts := handle.Timeseries(alias)
	require.NoError(t, ts.Create(24*time.Hour, cols...))
	t.Cleanup(func() { _ = ts.Remove() })

	return s
}

// writerColumns lists the columns in the order the point slices use.
func (s allColumnsSchema) writerColumns() []WriterColumn {
	return []WriterColumn{
		{ColumnName: s.blob, ColumnType: TsColumnBlob},
		{ColumnName: s.double, ColumnType: TsColumnDouble},
		{ColumnName: s.int64, ColumnType: TsColumnInt64},
		{ColumnName: s.str, ColumnType: TsColumnString},
		{ColumnName: s.timestamp, ColumnType: TsColumnTimestamp},
		{ColumnName: s.symbol, ColumnType: TsColumnSymbol},
	}
}

// genValidMask draws one bit per row, set with probability sparsity/100, so
// 100 writes every cell and 0 writes none, as sparsify does in the Python
// bindings' test suite.
func genValidMask(rng *rand.Rand, count int64, sparsity int) []bool {
	mask := make([]bool, count)
	for i := range mask {
		mask[i] = rng.Intn(100) < sparsity
	}

	return mask
}

// genAllColumnsData produces count rows of deterministic values ten seconds
// apart, starting at 1970-01-01T00:00:10Z, plus a validity mask per column.
func genAllColumnsData(rng *rand.Rand, count int64, sparsity int) TestTimeseriesData {
	td := TestTimeseriesData{
		BlobPoints:      make([]TsBlobPoint, count),
		DoublePoints:    make([]TsDoublePoint, count),
		Int64Points:     make([]TsInt64Point, count),
		StringPoints:    make([]TsStringPoint, count),
		TimestampPoints: make([]TsTimestampPoint, count),
		SymbolPoints:    make([]TsStringPoint, count),
		BlobValid:       genValidMask(rng, count, sparsity),
		DoubleValid:     genValidMask(rng, count, sparsity),
		Int64Valid:      genValidMask(rng, count, sparsity),
		StringValid:     genValidMask(rng, count, sparsity),
		TimestampValid:  genValidMask(rng, count, sparsity),
		SymbolValid:     genValidMask(rng, count, sparsity),
	}

	for i := range count {
		tsVal := time.Unix((i+1)*10, 0)
		content := fmt.Sprintf("content_%d", i)
		td.BlobPoints[i] = NewTsBlobPoint(tsVal, []byte(content))
		td.DoublePoints[i] = NewTsDoublePoint(tsVal, float64(i))
		td.Int64Points[i] = NewTsInt64Point(tsVal, i)
		td.StringPoints[i] = NewTsStringPoint(tsVal, content)
		td.TimestampPoints[i] = NewTsTimestampPoint(tsVal, tsVal)
		td.SymbolPoints[i] = NewTsStringPoint(tsVal, content)
	}

	return td
}

// pointValues extracts the value of each point, substituting null where the
// mask is clear. The writer has no separate null mask: writing the
// QDB_IS_NULL_* sentinel is how a null is written.
func pointValues[P, V any](points []P, valid []bool, get func(P) V, null V) []V {
	out := make([]V, len(points))
	for i, p := range points {
		if valid[i] {
			out[i] = get(p)
		} else {
			out[i] = null
		}
	}

	return out
}

// pushAllColumns writes td through the batch writer. Null timestamps are
// set on the raw timespec slice: the sentinel is qdb_min_time in both
// fields, and time.Time normalises such a nanosecond value into seconds, so
// NewColumnDataTimestamp cannot express it.
func pushAllColumns(t *testing.T, handle HandleType, schema allColumnsSchema, td TestTimeseriesData) {
	t.Helper()

	writerTable, err := NewWriterTable(td.Alias, schema.writerColumns())
	require.NoError(t, err)
	timestamps := make([]time.Time, len(td.Int64Points))
	for i := range td.Int64Points {
		timestamps[i] = td.Int64Points[i].Timestamp()
	}
	writerTable.SetIndex(timestamps)

	blobData := NewColumnDataBlob(pointValues(td.BlobPoints, td.BlobValid, TsBlobPoint.Content, nil))
	doubleData := NewColumnDataDouble(pointValues(td.DoublePoints, td.DoubleValid, TsDoublePoint.Content, math.NaN()))
	int64Data := NewColumnDataInt64(pointValues(td.Int64Points, td.Int64Valid, TsInt64Point.Content, math.MinInt64))
	stringData := NewColumnDataString(pointValues(td.StringPoints, td.StringValid, TsStringPoint.Content, ""))
	timestampData := NewColumnDataTimestamp(pointValues(td.TimestampPoints, td.TimestampValid, TsTimestampPoint.Content, time.Time{}))
	symbolData := NewColumnDataString(pointValues(td.SymbolPoints, td.SymbolValid, TsStringPoint.Content, ""))
	for i, ok := range td.TimestampValid {
		if !ok {
			timestampData.xs[i] = C.qdb_timespec_t{tv_sec: C.qdb_time_t(math.MinInt64), tv_nsec: C.qdb_time_t(math.MinInt64)}
		}
	}
	require.NoError(t, writerTable.SetDatas([]ColumnData{&blobData, &doubleData, &int64Data, &stringData, &timestampData, &symbolData}))

	writer := NewWriterWithDefaultOptions()
	require.NoError(t, writer.SetTable(writerTable))
	require.NoError(t, writer.Push(handle))
}

// newTestTimeseriesAllColumnsSparse creates a time series with one column
// of every supported type, populates it with count rows of deterministic
// data of which each cell is written with probability sparsity/100 and as
// null otherwise, and registers automatic cleanup. The random source is
// seeded from count and sparsity so a failure reproduces.
//
// Verified against the cluster: a row whose value columns are all null is
// still returned by a query, every value cell as none; and an empty string
// pushed to a symbol column comes back as none, like a string column.
func newTestTimeseriesAllColumnsSparse(t *testing.T, handle HandleType, count int64, sparsity int) TestTimeseriesData {
	t.Helper()

	alias := generateAlias(16)
	schema := newAllColumnsSchema(t, handle, alias)
	td := genAllColumnsData(rand.New(rand.NewSource(count*1000+int64(sparsity))), count, sparsity)
	td.Alias = alias
	pushAllColumns(t, handle, schema, td)

	return td
}

// newTestTimeseriesAllColumns is the dense fixture: every cell written.
func newTestTimeseriesAllColumns(t *testing.T, handle HandleType, count int64) TestTimeseriesData {
	t.Helper()

	return newTestTimeseriesAllColumnsSparse(t, handle, count, 100)
}

// WithGC provides memory isolation for tests by invoking garbage collection
// before and after test execution. This ensures proper memory cleanup between
// tests by calling Go's garbage collector.
//
// Decision rationale:
//   - Ensures memory isolation between tests in high-memory scenarios
//   - Uses Go's garbage collector for memory management
//   - Logs timing metrics to track GC overhead
//
// Key assumptions:
//   - test function follows standard testing patterns
//
// Performance trade-offs:
//   - Adds GC overhead but ensures test reliability
//   - Acceptable cost for memory isolation in test environments
//
// Usage example:
//
//	func TestMyMemoryIntensiveFunction(t *testing.T) {
//		WithGC(t, "TestMyMemoryIntensiveFunction", func() {
//			// Your test code here
//		})
//	}
func WithGC(t testHelper, testName string, testFunc func()) {
	t.Helper()

	// Phase 1: Pre-test GC
	startTime := time.Now()
	performGC(t, testName, "pre-test")
	preGCDuration := time.Since(startTime)

	// Run the actual test
	testStartTime := time.Now()
	testFunc()
	testDuration := time.Since(testStartTime)

	// Phase 2: Post-test GC
	postGCStartTime := time.Now()
	performGC(t, testName, "post-test")
	postGCDuration := time.Since(postGCStartTime)

	// Log timing metrics
	t.Logf("GC timing for %s: pre-GC=%v, test=%v, post-GC=%v, total-GC=%v",
		testName,
		preGCDuration,
		testDuration,
		postGCDuration,
		preGCDuration+postGCDuration)
}

// performGC executes Go garbage collection.
// This helper consolidates all GC operations for consistency.
func performGC(t testHelper, testName, phase string) {
	t.Helper()

	// Go garbage collection - call twice to ensure finalizers run
	runtime.GC()
	runtime.GC()

	// Return memory to OS
	debug.FreeOSMemory()

	t.Logf("Performed %s GC for %s", phase, testName)
}

// WithGCAndHandle provides memory isolation for tests. This version maintains
// the same interface as the original but now only uses Go's garbage collection.
//
// Decision rationale:
//   - Maintains API compatibility for existing test code
//   - Uses only Go GC for memory management
//   - Used for tests that have access to database handles
//
// Usage example:
//
//	func TestMyDatabaseFunction(t *testing.T) {
//		handle := newTestHandle(t)
//
//		WithGCAndHandle(t, handle, "TestMyDatabaseFunction", func() {
//			// Your test code here
//		})
//	}
func WithGCAndHandle(t testHelper, handle HandleType, testName string, testFunc func()) {
	t.Helper()

	// Phase 1: Pre-test GC
	startTime := time.Now()
	performGC(t, testName, "pre-test")
	preGCDuration := time.Since(startTime)

	// Run the actual test
	testStartTime := time.Now()
	testFunc()
	testDuration := time.Since(testStartTime)

	// Phase 2: Post-test GC
	postGCStartTime := time.Now()
	performGC(t, testName, "post-test")
	postGCDuration := time.Since(postGCStartTime)

	// Log timing metrics
	t.Logf("GC timing for %s: pre-GC=%v, test=%v, post-GC=%v, total-GC=%v",
		testName,
		preGCDuration,
		testDuration,
		postGCDuration,
		preGCDuration+postGCDuration)
}

// -----------------------------------------------------------------
// Hand-built query result rows for converter unit tests
// -----------------------------------------------------------------

// testCellFunc fills one zeroed cell that already lives in C memory.
type testCellFunc func(t *testing.T, h HandleType, p *C.qdb_point_result_t)

// testCellNone yields a null cell: the only null encoding qdb_query emits.
func testCellNone() testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_type(p, C.qdb_query_result_none)
	}
}

// testCellTagged yields a cell with the given raw tag and a zero payload,
// for the array tags that have no Go constant.
func testCellTagged(tag int64) testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_type(p, C.qdb_query_result_value_type_t(tag))
	}
}

// testArrayTags returns the raw enum values of the array result types.
func testArrayTags() []int64 {
	return []int64{
		C.qdb_query_result_array_double,
		C.qdb_query_result_array_int64,
		C.qdb_query_result_array_blob,
		C.qdb_query_result_array_timestamp,
		C.qdb_query_result_array_string,
	}
}

func testCellInt64(v int64) testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_int64(p, C.qdb_int_t(v))
	}
}

func testCellDouble(v float64) testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_double(p, C.double(v))
	}
}

func testCellCount(v uint64) testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_count(p, C.qdb_size_t(v))
	}
}

func testCellTimestamp(sec, nsec int64) testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_timestamp(p, C.qdb_time_t(sec), C.qdb_time_t(nsec))
	}
}

// testCellString yields a string cell whose content is a separate C
// allocation, as in a real result. An empty string is a nil pointer with
// length zero: qdbAllocAndCopyBytes rejects an empty slice, and the C API
// reports an empty cell the same way.
func testCellString(s string) testCellFunc {
	return func(t *testing.T, h HandleType, p *C.qdb_point_result_t) {
		t.Helper()
		if s == "" {
			C.set_point_string(p, nil, 0)

			return
		}

		content := qdbAllocAndCopyBytes(h, []byte(s))
		t.Cleanup(releaseCPtr(h, content))
		C.set_point_string(p, (*C.char)(content), C.qdb_size_t(len(s)))
	}
}

// testCellBlob is testCellString for blob cells.
func testCellBlob(b []byte) testCellFunc {
	return func(t *testing.T, h HandleType, p *C.qdb_point_result_t) {
		t.Helper()
		if len(b) == 0 {
			C.set_point_blob(p, nil, 0)

			return
		}

		content := qdbAllocAndCopyBytes(h, b)
		t.Cleanup(releaseCPtr(h, content))
		C.set_point_blob(p, content, C.qdb_size_t(len(b)))
	}
}

// testCellBlobUnbacked yields a blob cell with a nil pointer and a declared
// length of n bytes, for size-check tests only: the content must never be
// read.
func testCellBlobUnbacked(n uint64) testCellFunc {
	return func(_ *testing.T, _ HandleType, p *C.qdb_point_result_t) {
		C.set_point_blob(p, nil, C.qdb_size_t(n))
	}
}

// testCellSeq applies fills in order to the same cell, so a test can leave
// a stale payload under a later tag (a none cell over blob content).
func testCellSeq(fills ...testCellFunc) testCellFunc {
	return func(t *testing.T, h HandleType, p *C.qdb_point_result_t) {
		t.Helper()
		for _, fill := range fills {
			fill(t, h, p)
		}
	}
}

// newTestPointRows lays out rows in C memory in the shape qdb_query
// produces: an array of row pointers, each row one contiguous buffer of
// cells. Every buffer is released through t.Cleanup. The result is a
// QueryRows view, the same type rowsUnsafe returns.
func newTestPointRows(t *testing.T, h HandleType, rows [][]testCellFunc) QueryRows {
	t.Helper()
	if len(rows) == 0 {
		return QueryRows{}
	}

	rowPtrs := qdbAllocBufferZeroed[*C.qdb_point_result_t](h, len(rows))
	t.Cleanup(releaseCPtr(h, unsafe.Pointer(rowPtrs)))
	ptrs := unsafe.Slice(rowPtrs, len(rows))

	for i, row := range rows {
		require.NotEmpty(t, row, "row %d has no cells", i)

		// Each row is its own allocation so its cells are contiguous, which
		// is what the converter relies on when it slices a row.
		base := qdbAllocBufferZeroed[C.qdb_point_result_t](h, len(row))
		t.Cleanup(releaseCPtr(h, unsafe.Pointer(base)))
		cells := unsafe.Slice(base, len(row))
		for j, fill := range row {
			fill(t, h, &cells[j])
		}

		// ptrs backs onto C memory: a plain assignment would emit a Go
		// write barrier over it, so the store goes through setCPtr.
		setCPtr(unsafe.Pointer(&ptrs[i]), unsafe.Pointer(base))
	}

	return qdbPointResultStarArrayToSlice(rowPtrs, int64(len(rows)))
}
