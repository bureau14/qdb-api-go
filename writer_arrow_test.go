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

// genArrowIndex draws a strictly ascending index on the unit's grid, so no
// two rows share a timestamp after the unit truncation.
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

// genArrowTableSpec draws a table shape and its rows (possibly none) without
// touching the server. The alias is a placeholder.
func genArrowTableSpec(t *rapid.T, minRows int) arrowTestTable {
	cols := genArrowColumns(t)
	unit := rapid.SampledFrom(arrowIndexTypes).Draw(t, "indexType")
	rows := rapid.IntRange(minRows, 32).Draw(t, "rowCount")
	cells := make([][]arrowCell, len(cols))
	for j, col := range cols {
		cells[j] = make([]arrowCell, rows)
		for i := range rows {
			cells[j][i] = genArrowCell(t, col.ColumnType, arrowUnitDuration(unit))
		}
	}

	return arrowTestTable{alias: "t", cols: cols, unit: unit, idx: genArrowIndex(t, rows, arrowUnitDuration(unit)), cells: cells}
}

// genArrowTestTable draws a table and creates it on the server.
func genArrowTestTable(t *rapid.T, handle HandleType) arrowTestTable {
	tt := genArrowTableSpec(t, 0)
	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, tt.cols)
	require.NoError(t, err)
	tt.alias = tbl.alias

	return tt
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
	// Cut points are drawn one after another, each at or after the previous
	// one, so batches may be empty and the last one always ends at rows.
	// Empty middle batches are wanted: the C side must concatenate them away.
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
// rows of tt.
func assertArrowTableReadsBack(t testHelper, handle HandleType, tt arrowTestTable) {
	t.Helper()

	// The reader returns rows in its own batch layout and order, so the
	// comparison goes through the index: the total row count must match,
	// and every read-back row is matched to the drawn row with the same
	// "$timestamp" (unique by construction of genArrowIndex) and compared
	// cell by cell. Schema position 0 is "$table", 1 is "$timestamp".
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

// genArrowWriterOptions draws a push mode and a dedup setting; the bool
// says whether dedup is on. Async is left out because its rows are not
// readable right after Push returns.
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

// dropColumn returns rec without column i. The arrays are shared; the
// caller releases the result.
func dropColumn(rec arrow.RecordBatch, i int) arrow.RecordBatch { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	fields := append(append([]arrow.Field{}, rec.Schema().Fields()[:i]...), rec.Schema().Fields()[i+1:]...)
	cols := append(append([]arrow.Array{}, rec.Columns()[:i]...), rec.Columns()[i+1:]...)

	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
}

// repushFromReader reads tt back and pushes the C-allocated batches into a
// fresh table with the same columns, then checks that table.
func repushFromReader(t testHelper, handle HandleType, tt arrowTestTable) {
	t.Helper()

	// Batches from Reader.Arrow have their buffers in C memory, where
	// Pinner.Pin is a no-op. Pushing them again covers that path. The
	// reader's "$table" column is dropped first because SetTable refuses it.
	recs := readBackArrow(t, handle, tt.alias)
	defer releaseRecords(recs)

	tbl, err := createTableOfWriterColumnsAndDefaultShardSize(handle, tt.cols)
	require.NoError(t, err)

	w := NewArrowWriterWithDefaultOptions()
	stripped := make([]arrow.RecordBatch, len(recs))
	for i, rec := range recs {
		stripped[i] = dropColumn(rec, 0)
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
			// One run is one push of 1-3 drawn tables, each with its own
			// column set, index unit, row count (possibly zero) and batch
			// split, under drawn push options:
			//  1. draw and create the tables, build their batches;
			//  2. push once; with dedup on, push the same batches again,
			//     which must add nothing;
			//  3. release the batches before reading back: Push must not
			//     have kept anything of the caller's;
			//  4. read every table back and compare cell by cell, which
			//     also shows zero-row tables as absent, not failed;
			//  5. re-push one live table from the reader's output to cover
			//     buffers that live in C memory.
			tableCount := rapid.IntRange(1, 3).Draw(rt, "tableCount")
			tables := make([]arrowTestTable, tableCount)
			batches := make([][]arrow.RecordBatch, tableCount)

			// 1. tables and batches
			for i := range tableCount {
				tables[i] = genArrowTestTable(rt, handle)
				batches[i] = genArrowBatches(rt, tables[i])
			}
			opts, dedup := genArrowWriterOptions(rt)

			// 2. push, twice under dedup
			pushArrowTables(rt, handle, opts, tables, batches)
			if dedup {
				pushArrowTables(rt, handle, opts, tables, batches)
			}

			// 3. caller releases
			for _, bs := range batches {
				releaseRecords(bs)
			}

			// 4. read back
			for _, tt := range tables {
				assertArrowTableReadsBack(rt, handle, tt)
			}

			// 5. re-push from C memory
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

// arrowRejection is one way to break a valid table so that SetTable must
// refuse it. name selects the mutation in mutateArrowTable; code is the
// ErrorType the refusal must carry. Each name maps to one rule in
// writer_arrow.go: the schema rules of validateArrowSchema, the batch rules
// of validateArrowBatches, and the name rules of SetTable itself.
type arrowRejection struct {
	name string
	code ErrorType
}

var arrowRejections = []arrowRejection{
	{"no_timestamp", ErrInvalidArgument},          // schema rule 1
	{"timestamp_wrong_type", ErrIncompatibleType}, // schema rule 1
	{"table_column", ErrInvalidArgument},          // schema rule 2
	{"large_utf8", ErrIncompatibleType},           // schema rule 3
	{"dictionary", ErrIncompatibleType},           // schema rule 3
	{"list", ErrIncompatibleType},                 // schema rule 3
	{"schema_mismatch", ErrInvalidArgument},       // batch rule 2
	{"timestamp_null", ErrInvalidArgument},        // batch rule 3
	{"no_batches", ErrInvalidArgument},            // batch rule, empty set
	{"duplicate_table", ErrInvalidArgument},       // SetTable name rule
	{"empty_name", ErrInvalidArgument},            // SetTable name rule
}

// constantColumn builds n rows of one value in an unsupported or reserved
// type. These types are never drawn, so they need their own builder.
func constantColumn(t testHelper, dt arrow.DataType, n int) arrow.Array { //nolint:ireturn // Justified: arrow.Array is arrow-go's array interface
	t.Helper()

	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	for range n {
		switch bb := b.(type) {
		case *array.Int64Builder:
			bb.Append(1)
		case *array.StringBuilder:
			bb.Append("a")
		case *array.LargeStringBuilder:
			bb.Append("a")
		case *array.BinaryDictionaryBuilder:
			require.NoError(t, bb.AppendString("a"))
		case *array.ListBuilder:
			bb.Append(true)
			bb.ValueBuilder().(*array.Int64Builder).Append(1)
		default:
			require.Failf(t, "unhandled builder", "%T", bb)
		}
	}

	return b.NewArray()
}

// withColumn returns rec with column i replaced by arr under field f. The
// other arrays are shared; the caller releases the result.
func withColumn(rec arrow.RecordBatch, i int, f arrow.Field, arr arrow.Array) arrow.RecordBatch { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	fields := append([]arrow.Field{}, rec.Schema().Fields()...)
	cols := append([]arrow.Array{}, rec.Columns()...)
	fields[i] = f
	cols[i] = arr

	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
}

// withExtraColumn returns rec with arr appended under field f. The other
// arrays are shared; the caller releases the result.
func withExtraColumn(rec arrow.RecordBatch, f arrow.Field, arr arrow.Array) arrow.RecordBatch { //nolint:ireturn // Justified: arrow.RecordBatch is arrow-go's batch interface
	fields := append(append([]arrow.Field{}, rec.Schema().Fields()...), f)
	cols := append(append([]arrow.Array{}, rec.Columns()...), arr)

	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rec.NumRows())
}

// stageWithExtraColumn appends one column of a reserved name or an
// unsupported type and stages the result.
func stageWithExtraColumn(t testHelper, w *ArrowWriter, rec arrow.RecordBatch, name string) error {
	t.Helper()

	n := int(rec.NumRows())
	var f arrow.Field
	switch name {
	case "table_column":
		f = arrow.Field{Name: "$table", Type: arrow.BinaryTypes.String}
	case "large_utf8":
		f = arrow.Field{Name: "x", Type: arrow.BinaryTypes.LargeString, Nullable: true}
	case "dictionary":
		f = arrow.Field{Name: "x", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}, Nullable: true}
	case "list":
		f = arrow.Field{Name: "x", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true}
	default:
		require.Failf(t, "unknown rejection", "%s", name)
	}
	arr := constantColumn(t, f.Type, n)
	defer arr.Release()
	bad := withExtraColumn(rec, f, arr)
	defer bad.Release()

	return w.SetTable("t", bad)
}

// mutateArrowTable applies the named mutation to a valid batch and stages
// the result, returning SetTable's error. rec has "$timestamp" at 0 and at
// least one row.
func mutateArrowTable(t testHelper, w *ArrowWriter, rec arrow.RecordBatch, name string) error {
	t.Helper()

	n := int(rec.NumRows())
	switch name {
	case "no_timestamp":
		// Drop the index column; what remains is a valid data-only schema.
		bad := dropColumn(rec, 0)
		defer bad.Release()

		return w.SetTable("t", bad)
	case "timestamp_wrong_type":
		// Keep the name, change the type to int64.
		arr := constantColumn(t, arrow.PrimitiveTypes.Int64, n)
		defer arr.Release()
		bad := withColumn(rec, 0, arrow.Field{Name: "$timestamp", Type: arr.DataType()}, arr)
		defer bad.Release()

		return w.SetTable("t", bad)
	case "timestamp_null":
		// Same index type, every slot null.
		arr := array.MakeArrayOfNull(memory.DefaultAllocator, rec.Schema().Field(0).Type, n)
		defer arr.Release()
		bad := withColumn(rec, 0, rec.Schema().Field(0), arr)
		defer bad.Release()

		return w.SetTable("t", bad)
	case "schema_mismatch":
		// A second batch with one column fewer; both schemas are valid on
		// their own, they only disagree with each other.
		other := dropColumn(rec, int(rec.NumCols())-1)
		defer other.Release()

		return w.SetTable("t", rec, other)
	case "no_batches":
		return w.SetTable("t")
	case "duplicate_table":
		require.NoError(t, w.SetTable("dup", rec))

		return w.SetTable("dup", rec)
	case "empty_name":
		return w.SetTable("", rec)
	default:
		return stageWithExtraColumn(t, w, rec, name)
	}
}

func TestArrowWriterRejectsInvalidInput(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Draw a valid table with at least one row, build it as one batch,
		// then apply one drawn mutation. The mutation must be refused with
		// its ErrorType, and the writer must hold what it held before: the
		// sane table, plus one more for the duplicate case.
		tt := genArrowTableSpec(rt, 1)
		rec := buildArrowBatch(tt, 0, len(tt.idx))
		defer rec.Release()
		rj := rapid.SampledFrom(arrowRejections).Draw(rt, "rejection")

		w := NewArrowWriterWithDefaultOptions()
		require.NoError(rt, w.SetTable("sane", rec), "the unmutated table must be accepted")
		before := w.Length()
		if rj.name == "duplicate_table" {
			before++
		}

		err := mutateArrowTable(rt, &w, rec, rj.name)
		require.Error(rt, err, rj.name)
		assert.ErrorIs(rt, err, rj.code, rj.name)
		assert.Equal(rt, before, w.Length(), rj.name)
	})
}

func TestArrowWriterUnknownTableIsAliasNotFound(t *testing.T) {
	handle := newTestHandle(t)
	rapid.Check(t, func(rt *rapid.T) {
		tt := genArrowTableSpec(rt, 1)
		rec := buildArrowBatch(tt, 0, len(tt.idx))
		defer rec.Release()

		w := NewArrowWriterWithDefaultOptions()
		require.NoError(rt, w.SetTable(generateDefaultAlias(), rec))
		err := w.Push(handle)
		require.Error(rt, err)
		assert.ErrorIs(rt, err, ErrAliasNotFound)
	})
}
