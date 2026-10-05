package qdb

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// arrowCell is one drawn value; only the field for its column type is set.
type arrowCell struct {
	valid bool
	i     int64
	f     float64
	t     time.Time
	s     string
	b     []byte
}

// arrowTestTable is one drawn table: its server alias, columns, index type,
// index and cells, indexed [column][row].
type arrowTestTable struct {
	alias string
	cols  []WriterColumn
	unit  arrow.DataType
	idx   []time.Time
	cells [][]arrowCell
}

var arrowIndexTypes = []arrow.DataType{
	arrow.FixedWidthTypes.Timestamp_s,
	arrow.FixedWidthTypes.Timestamp_ms,
	arrow.FixedWidthTypes.Timestamp_us,
	arrow.FixedWidthTypes.Timestamp_ns,
	arrow.FixedWidthTypes.Date64,
}

var arrowWriterColumnTypes = []TsColumnType{TsColumnInt64, TsColumnDouble, TsColumnTimestamp, TsColumnBlob, TsColumnString, TsColumnSymbol}

// arrowUnitDuration is the granularity of an index type.
func arrowUnitDuration(dt arrow.DataType) time.Duration {
	if ts, ok := dt.(*arrow.TimestampType); ok {
		return ts.Unit.Multiplier()
	}

	return time.Millisecond // date64
}

// arrowDataType maps a table column type to the Arrow type pushed for it.
func arrowDataType(ctype TsColumnType, unit arrow.DataType) arrow.DataType { //nolint:ireturn // Justified: arrow.DataType is arrow-go's type interface
	switch ctype {
	case TsColumnInt64:
		return arrow.PrimitiveTypes.Int64
	case TsColumnDouble:
		return arrow.PrimitiveTypes.Float64
	case TsColumnTimestamp:
		return unit
	case TsColumnBlob:
		return arrow.BinaryTypes.Binary
	case TsColumnString, TsColumnSymbol:
		return arrow.BinaryTypes.String
	case TsColumnUninitialized:
		return arrow.Null
	}

	return arrow.Null
}

// genArrowColumns draws 1-4 columns with unique names over all six types.
func genArrowColumns(t *rapid.T) []WriterColumn {
	n := rapid.IntRange(1, 4).Draw(t, "columnCount")
	seen := map[string]bool{}
	cols := make([]WriterColumn, 0, n)
	for len(cols) < n {
		col := genWriterColumnOfType(t, rapid.SampledFrom(arrowWriterColumnTypes).Draw(t, "columnType"))
		if seen[col.ColumnName] {
			continue
		}
		seen[col.ColumnName] = true
		cols = append(cols, col)
	}

	return cols
}

// genArrowIndex draws a strictly ascending index on the unit's grid.
func genArrowIndex(t *rapid.T, rows int, unit time.Duration) []time.Time {
	start := genTime(t).Truncate(unit)
	step := time.Duration(rapid.Int64Range(1, 1000).Draw(t, "step")) * unit
	idx := make([]time.Time, rows)
	for i := range rows {
		idx[i] = start.Add(step * time.Duration(i))
	}

	return idx
}

// genArrowCell draws one cell. Sentinel values (MinInt64, NaN) are avoided
// because the server reads them as null; empty strings and blobs are drawn
// on purpose because the server stores them as null too, which the
// read-back check expects. Symbols are never empty.
func genArrowCell(t *rapid.T, ctype TsColumnType, unit time.Duration) arrowCell {
	c := arrowCell{valid: rapid.Bool().Draw(t, "valid")}
	if !c.valid {
		return c
	}
	switch ctype {
	case TsColumnInt64:
		c.i = rapid.Int64Range(math.MinInt64+1, math.MaxInt64).Draw(t, "int64")
	case TsColumnDouble:
		c.f = rapid.Float64Range(-1e12, 1e12).Draw(t, "double")
	case TsColumnTimestamp:
		c.t = genTime(t).Truncate(unit)
	case TsColumnBlob:
		c.b = rapid.SliceOfN(rapid.Byte(), 0, 16).Draw(t, "blob")
	case TsColumnString:
		c.s = rapid.StringOfN(rapid.RuneFrom([]rune("abcxyz019 ")), 0, 16, -1).Draw(t, "string")
	case TsColumnSymbol:
		c.s = rapid.StringOfN(rapid.RuneFrom([]rune("abcxyz019")), 1, 16, -1).Draw(t, "symbol")
	case TsColumnUninitialized:
	}

	return c
}

// genArrowTestTable draws a table, creates it on the server and draws its
// rows (possibly zero).
func genArrowTestTable(t *rapid.T, handle HandleType) arrowTestTable {
	cols := genArrowColumns(t)
	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, cols)
	require.NoError(t, err)

	unit := rapid.SampledFrom(arrowIndexTypes).Draw(t, "indexType")
	rows := rapid.IntRange(0, 32).Draw(t, "rowCount")
	cells := make([][]arrowCell, len(cols))
	for j, col := range cols {
		cells[j] = make([]arrowCell, rows)
		for i := range rows {
			cells[j][i] = genArrowCell(t, col.ColumnType, arrowUnitDuration(unit))
		}
	}

	return arrowTestTable{alias: tbl.alias, cols: cols, unit: unit, idx: genArrowIndex(t, rows, arrowUnitDuration(unit)), cells: cells}
}

// arrowSchemaOf is the schema pushed for the table: "$timestamp" first.
func arrowSchemaOf(tt arrowTestTable) *arrow.Schema {
	fields := make([]arrow.Field, 0, 1+len(tt.cols))
	fields = append(fields, arrow.Field{Name: "$timestamp", Type: tt.unit})
	for _, col := range tt.cols {
		fields = append(fields, arrow.Field{Name: col.ColumnName, Type: arrowDataType(col.ColumnType, tt.unit), Nullable: true})
	}

	return arrow.NewSchema(fields, nil)
}

// appendArrowTime appends a time to a timestamp or date64 builder.
func appendArrowTime(b array.Builder, dt arrow.DataType, v time.Time) {
	switch bb := b.(type) {
	case *array.TimestampBuilder:
		bb.Append(arrow.Timestamp(v.UnixNano() / int64(arrowUnitDuration(dt))))
	case *array.Date64Builder:
		bb.Append(arrow.Date64(v.UnixMilli()))
	}
}

// appendArrowCell appends one cell to the builder of its column.
func appendArrowCell(b array.Builder, dt arrow.DataType, c arrowCell) {
	if !c.valid {
		b.AppendNull()

		return
	}
	switch bb := b.(type) {
	case *array.Int64Builder:
		bb.Append(c.i)
	case *array.Float64Builder:
		bb.Append(c.f)
	case *array.StringBuilder:
		bb.Append(c.s)
	case *array.BinaryBuilder:
		bb.Append(c.b)
	default:
		appendArrowTime(b, dt, c.t)
	}
}

// buildArrowBatch builds rows [from, to) of the table as one batch. The
// caller releases it.
func buildArrowBatch(tt arrowTestTable, from, to int) arrow.RecordBatch { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	schema := arrowSchemaOf(tt)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for i := from; i < to; i++ {
		appendArrowTime(b.Field(0), tt.unit, tt.idx[i])
		for j := range tt.cols {
			appendArrowCell(b.Field(1+j), schema.Field(1+j).Type, tt.cells[j][i])
		}
	}

	return b.NewRecordBatch()
}

// genArrowBatches splits the table's rows into 1-4 batches.
func genArrowBatches(t *rapid.T, tt arrowTestTable) []arrow.RecordBatch {
	rows := len(tt.idx)
	parts := rapid.IntRange(1, 4).Draw(t, "batchCount")
	var recs []arrow.RecordBatch
	from := 0
	for p := range parts {
		to := rows
		if p < parts-1 {
			to = rapid.IntRange(from, rows).Draw(t, "batchEnd")
		}
		recs = append(recs, buildArrowBatch(tt, from, to))
		from = to
	}

	return recs
}

// cellReadsBackNull reports whether the server answers null for the drawn
// cell: a null slot, or a zero-length string or blob, which the server
// cannot tell apart from null.
func cellReadsBackNull(arr arrow.Array, c arrowCell) bool {
	if !c.valid {
		return true
	}
	switch arr.(type) {
	case *array.String:
		return c.s == ""
	case *array.Binary:
		return len(c.b) == 0
	default:
		return false
	}
}

// assertArrowCellEqualsExpected compares one read-back slot with the drawn
// cell. Read-back timestamps are always nanoseconds.
func assertArrowCellEqualsExpected(t testHelper, arr arrow.Array, i int, c arrowCell) {
	t.Helper()

	if cellReadsBackNull(arr, c) {
		assert.True(t, arr.IsNull(i), "slot %d is null", i)

		return
	}
	require.True(t, arr.IsValid(i), "slot %d is valid", i)
	switch a := arr.(type) {
	case *array.Int64:
		assert.Equal(t, c.i, a.Value(i))
	case *array.Float64:
		assert.Equal(t, c.f, a.Value(i))
	case *array.Timestamp:
		assert.Equal(t, c.t.UnixNano(), int64(a.Value(i)))
	case *array.String:
		assert.Equal(t, c.s, a.Value(i))
	case *array.Binary:
		assert.Equal(t, c.b, a.Value(i))
	default:
		require.Failf(t, "unexpected array", "%T", arr)
	}
}

// readBackArrow drains Reader.Arrow for one table. The caller releases the
// records.
func readBackArrow(t testHelper, handle HandleType, alias string) []arrow.RecordBatch {
	t.Helper()

	reader, err := NewReader(handle, NewReaderDefaultOptions([]string{alias}))
	require.NoError(t, err)
	defer reader.Close()

	return collectArrow(t, &reader)
}

// assertArrowTableReadsBack checks that the server holds exactly the drawn
// rows of tt, matching rows on "$timestamp".
func assertArrowTableReadsBack(t testHelper, handle HandleType, tt arrowTestTable) {
	t.Helper()

	recs := readBackArrow(t, handle, tt.alias)
	defer releaseRecords(recs)
	require.Equal(t, int64(len(tt.idx)), totalRows(recs), "row count of %s", tt.alias)

	byStamp := make(map[int64]int, len(tt.idx))
	for k, v := range tt.idx {
		byStamp[v.UnixNano()] = k
	}
	for _, rec := range recs {
		assertLegacyArrowSchema(t, rec.Schema(), tt.cols)
		stamps, ok := rec.Column(1).(*array.Timestamp)
		require.True(t, ok)
		for i := range int(rec.NumRows()) {
			k, found := byStamp[int64(stamps.Value(i))]
			require.True(t, found, "timestamp %v not in index", stamps.Value(i))
			for j := range tt.cols {
				assertArrowCellEqualsExpected(t, rec.Column(2+j), i, tt.cells[j][k])
			}
		}
	}
}

// genArrowWriterOptions draws a push mode and a dedup setting. Async is
// left out because its rows are not readable right after Push returns.
func genArrowWriterOptions(t *rapid.T) (WriterOptions, bool) {
	opts := NewWriterOptions().WithPushMode(rapid.SampledFrom([]WriterPushMode{WriterPushModeTransactional, WriterPushModeFast}).Draw(t, "pushMode"))
	switch rapid.SampledFrom([]string{"off", "drop", "upsert"}).Draw(t, "dedup") {
	case "drop":
		return opts.EnableDropDuplicatesOn([]string{"$timestamp"}), true
	case "upsert":
		return opts.WithDeduplicationMode(WriterDeduplicationModeUpsert).EnableDropDuplicatesOn([]string{"$timestamp"}), true
	default:
		return opts, false
	}
}

// pushArrowTables stages every table's batches and pushes once.
func pushArrowTables(t testHelper, handle HandleType, opts WriterOptions, tables []arrowTestTable, batches [][]arrow.RecordBatch) {
	t.Helper()

	w := NewArrowWriter(opts)
	for i, tt := range tables {
		require.NoError(t, w.SetTable(tt.alias, batches[i]...))
	}
	require.Equal(t, len(tables), w.Length())
	require.NoError(t, w.Push(handle))
}

// withoutTableColumn drops the reader's "$table" column so the batch can be
// pushed again. The arrays are shared; the caller releases the result.
func withoutTableColumn(rec arrow.RecordBatch) arrow.RecordBatch { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	fields := rec.Schema().Fields()[1:]

	return array.NewRecordBatch(arrow.NewSchema(fields, nil), rec.Columns()[1:], rec.NumRows())
}

// repushFromReader reads tt back and pushes the C-allocated batches into a
// fresh table with the same columns, then checks that table.
func repushFromReader(t testHelper, handle HandleType, tt arrowTestTable) {
	t.Helper()

	recs := readBackArrow(t, handle, tt.alias)
	defer releaseRecords(recs)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, tt.cols)
	require.NoError(t, err)

	w := NewArrowWriterWithDefaultOptions()
	stripped := make([]arrow.RecordBatch, len(recs))
	for i, rec := range recs {
		stripped[i] = withoutTableColumn(rec)
	}
	defer releaseRecords(stripped)
	require.NoError(t, w.SetTable(tbl.alias, stripped...))
	require.NoError(t, w.Push(handle))

	copied := tt
	copied.alias = tbl.alias
	assertArrowTableReadsBack(t, handle, copied)
}

func TestArrowWriterRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		handle := newTestHandle(rt)

		WithGCAndHandle(rt, handle, "TestArrowWriterRoundTrip", func() {
			tableCount := rapid.IntRange(1, 3).Draw(rt, "tableCount")
			tables := make([]arrowTestTable, tableCount)
			batches := make([][]arrow.RecordBatch, tableCount)
			for i := range tableCount {
				tables[i] = genArrowTestTable(rt, handle)
				batches[i] = genArrowBatches(rt, tables[i])
			}
			opts, dedup := genArrowWriterOptions(rt)

			pushArrowTables(rt, handle, opts, tables, batches)
			if dedup {
				// The same rows again must not add anything.
				pushArrowTables(rt, handle, opts, tables, batches)
			}
			for _, bs := range batches {
				releaseRecords(bs)
			}

			for _, tt := range tables {
				assertArrowTableReadsBack(rt, handle, tt)
			}
			for _, tt := range tables {
				if len(tt.idx) > 0 {
					repushFromReader(rt, handle, tt)

					break
				}
			}
		})
	})
}

func TestArrowWriterPushWithoutRowsIsNoop(t *testing.T) {
	handle := newTestHandle(t)
	cols := generateWriterColumnsOfAllTypes()
	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, cols)
	require.NoError(t, err)

	tt := arrowTestTable{alias: tbl.alias, cols: cols, unit: arrow.FixedWidthTypes.Timestamp_ns, cells: make([][]arrowCell, len(cols))}
	rec := buildArrowBatch(tt, 0, 0)
	defer rec.Release()

	w := NewArrowWriterWithDefaultOptions()
	require.NoError(t, w.SetTable(tbl.alias, rec))
	// An unknown table next to it proves the C API is not called at all.
	require.NoError(t, w.SetTable(generateDefaultAlias(), rec))
	require.NoError(t, w.Push(handle))

	recs := readBackArrow(t, handle, tbl.alias)
	defer releaseRecords(recs)
	assert.Zero(t, totalRows(recs))
}

func TestArrowWriterAsyncPushSucceeds(t *testing.T) {
	handle := newTestHandle(t)
	rapid.Check(t, func(rt *rapid.T) {
		tt := genArrowTestTable(rt, handle)
		recs := genArrowBatches(rt, tt)
		defer releaseRecords(recs)

		w := NewArrowWriter(NewWriterOptions().WithAsyncPush())
		require.NoError(rt, w.SetTable(tt.alias, recs...))
		require.NoError(rt, w.Push(handle))
	})
}

// arrowRejectionCase is one mutation of a valid single-row table.
type arrowRejectionCase struct {
	name string
	code ErrorType
}

var arrowRejectionCases = []arrowRejectionCase{
	{"no_timestamp", ErrInvalidArgument},
	{"timestamp_wrong_type", ErrIncompatibleType},
	{"timestamp_null", ErrInvalidArgument},
	{"table_column", ErrInvalidArgument},
	{"large_utf8", ErrIncompatibleType},
	{"dictionary", ErrIncompatibleType},
	{"list", ErrIncompatibleType},
	{"schema_mismatch", ErrInvalidArgument},
	{"duplicate_table", ErrInvalidArgument},
	{"empty_name", ErrInvalidArgument},
	{"no_batches", ErrInvalidArgument},
}

// buildSingleRow builds a one-row batch for the schema, with every slot
// valid (or null when nullTs is set for field 0).
func buildSingleRow(t testHelper, schema *arrow.Schema, nullTs bool) arrow.RecordBatch { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	t.Helper()

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for j, f := range schema.Fields() {
		if j == 0 && nullTs {
			b.Field(0).AppendNull()

			continue
		}
		switch bb := b.Field(j).(type) {
		case *array.TimestampBuilder:
			bb.Append(1)
		case *array.Int64Builder:
			bb.Append(1)
		case *array.StringBuilder:
			bb.Append("a")
		case *array.LargeStringBuilder:
			bb.Append("a")
		case *array.BinaryDictionaryBuilder:
			_ = bb.AppendString("a")
		case *array.ListBuilder:
			bb.Append(true)
			bb.ValueBuilder().(*array.Int64Builder).Append(1)
		default:
			require.Failf(t, "unhandled builder", "%T for %s", bb, f.Name)
		}
	}

	return b.NewRecordBatch()
}

// arrowRejectionSchema returns the mutated schema for a case; nil means the
// valid schema is used.
func arrowRejectionSchema(name string) *arrow.Schema {
	ts := arrow.Field{Name: "$timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns}
	v := arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	switch name {
	case "no_timestamp":
		return arrow.NewSchema([]arrow.Field{v}, nil)
	case "timestamp_wrong_type":
		return arrow.NewSchema([]arrow.Field{{Name: "$timestamp", Type: arrow.PrimitiveTypes.Int64}, v}, nil)
	case "table_column":
		return arrow.NewSchema([]arrow.Field{ts, {Name: "$table", Type: arrow.BinaryTypes.String}, v}, nil)
	case "large_utf8":
		return arrow.NewSchema([]arrow.Field{ts, {Name: "s", Type: arrow.BinaryTypes.LargeString, Nullable: true}}, nil)
	case "dictionary":
		dict := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}

		return arrow.NewSchema([]arrow.Field{ts, {Name: "s", Type: dict, Nullable: true}}, nil)
	case "list":
		return arrow.NewSchema([]arrow.Field{ts, {Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true}}, nil)
	default:
		return arrow.NewSchema([]arrow.Field{ts, v}, nil)
	}
}

// stageRejectionCase runs SetTable for one case and returns its error.
func stageRejectionCase(t testHelper, w *ArrowWriter, c arrowRejectionCase) error {
	t.Helper()

	valid := buildSingleRow(t, arrowRejectionSchema("valid"), false)
	defer valid.Release()
	rec := buildSingleRow(t, arrowRejectionSchema(c.name), c.name == "timestamp_null")
	defer rec.Release()

	switch c.name {
	case "schema_mismatch":
		other := buildSingleRow(t, arrow.NewSchema([]arrow.Field{{Name: "$timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns}, {Name: "w", Type: arrow.PrimitiveTypes.Int64}}, nil), false)
		defer other.Release()

		return w.SetTable("t", valid, other)
	case "duplicate_table":
		err := w.SetTable("dup", valid)
		if err != nil {
			return err
		}

		return w.SetTable("dup", valid)
	case "empty_name":
		return w.SetTable("", valid)
	case "no_batches":
		return w.SetTable("t")
	default:
		return w.SetTable("t", rec)
	}
}

func TestArrowWriterRejectsInvalidInput(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		c := rapid.SampledFrom(arrowRejectionCases).Draw(rt, "case")
		w := NewArrowWriterWithDefaultOptions()
		before := w.Length()
		if c.name == "duplicate_table" {
			before = 1
		}

		err := stageRejectionCase(rt, &w, c)
		require.Error(rt, err, c.name)
		assert.ErrorIs(rt, err, c.code, c.name)
		assert.Equal(rt, before, w.Length(), c.name)
	})
}

func TestArrowWriterUnknownTableIsAliasNotFound(t *testing.T) {
	handle := newTestHandle(t)
	rec := buildSingleRow(t, arrowRejectionSchema("valid"), false)
	defer rec.Release()

	w := NewArrowWriterWithDefaultOptions()
	require.NoError(t, w.SetTable(generateDefaultAlias(), rec))
	err := w.Push(handle)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAliasNotFound)
}
