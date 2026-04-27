package qdb

import (
	"errors"
	"testing"
	"time"
)

func TestEnsureTimestampColumnFirstAddsMissingColumn(t *testing.T) {
	cols, err := ensureTimestampColumnFirst(
		NewTsColumnInfo("value", TsColumnDouble),
		NewTsColumnInfo("volume", TsColumnInt64),
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}
	if cols[0].Name() != tsTimestampColumnName {
		t.Fatalf("expected first column to be %q, got %q", tsTimestampColumnName, cols[0].Name())
	}
	if cols[0].Type() != TsColumnTimestamp {
		t.Fatalf("expected first column type to be %v, got %v", TsColumnTimestamp, cols[0].Type())
	}
	if cols[1].Name() != "value" || cols[2].Name() != "volume" {
		t.Fatalf("expected original columns order to be preserved after timestamp insertion, got %+v", cols)
	}
}

func TestEnsureTimestampColumnFirstAcceptsValidFirstColumn(t *testing.T) {
	cols, err := ensureTimestampColumnFirst(
		NewTsColumnInfo(tsTimestampColumnName, TsColumnTimestamp),
		NewTsColumnInfo("value", TsColumnDouble),
		NewTsColumnInfo("volume", TsColumnInt64),
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cols[0].Name() != tsTimestampColumnName {
		t.Fatalf("expected first column to be %q, got %q", tsTimestampColumnName, cols[0].Name())
	}
	if cols[0].Type() != TsColumnTimestamp {
		t.Fatalf("expected first column type to be %v, got %v", TsColumnTimestamp, cols[0].Type())
	}
}

func TestEnsureTimestampColumnFirstRejectsNonFirstColumn(t *testing.T) {
	_, err := ensureTimestampColumnFirst(
		NewTsColumnInfo("value", TsColumnDouble),
		NewTsColumnInfo(tsTimestampColumnName, TsColumnTimestamp),
	)

	if err == nil {
		t.Fatal("expected error for non-first timestamp column, got nil")
	}
}

func TestEnsureTimestampColumnFirstRejectsWrongType(t *testing.T) {
	_, err := ensureTimestampColumnFirst(
		NewTsColumnInfo(tsTimestampColumnName, TsColumnString),
		NewTsColumnInfo("value", TsColumnDouble),
	)

	if err == nil {
		t.Fatal("expected error for wrong timestamp column type, got nil")
	}
}

func TestEnsureTimestampColumnFirstRejectsSymtable(t *testing.T) {
	_, err := ensureTimestampColumnFirst(
		NewSymbolColumnInfo(tsTimestampColumnName, "symtable"),
		NewTsColumnInfo("value", TsColumnDouble),
	)

	if err == nil {
		t.Fatal("expected error for timestamp column with symtable, got nil")
	}
}

func TestTimeseriesColumnsInfoReturnsCreatedColumns(t *testing.T) {
	entry, expectedColumns := newTestTimeseriesMetadataTable(t)

	columns, err := entry.ColumnsInfo()
	if err != nil {
		t.Fatalf("expected ColumnsInfo to succeed, got %v", err)
	}

	assertColumnInfos(t, columns, expectedColumns)
}

func TestTimeseriesColumnsInfoReturnsAliasNotFound(t *testing.T) {
	handle := newTestHandle(t)
	entry := handle.Timeseries(generateAlias(16))

	columns, err := entry.ColumnsInfo()
	if !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected ErrAliasNotFound, got columns=%v err=%v", columns, err)
	}
	if columns != nil {
		t.Fatalf("expected nil columns on error, got %+v", columns)
	}
}

func TestTimeseriesColumnsReturnsTypedColumns(t *testing.T) {
	entry, expectedColumns := newTestTimeseriesMetadataTable(t)

	blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, err := entry.Columns()
	if err != nil {
		t.Fatalf("expected Columns to succeed, got %v", err)
	}

	assertColumns(t, blobColumns, columnInfosOfTypes(expectedColumns, TsColumnBlob))
	assertColumns(t, doubleColumns, columnInfosOfTypes(expectedColumns, TsColumnDouble))
	assertColumns(t, int64Columns, columnInfosOfTypes(expectedColumns, TsColumnInt64))
	assertColumns(t, stringColumns, columnInfosOfTypes(expectedColumns, TsColumnString, TsColumnSymbol))
	assertColumns(t, timestampColumns, columnInfosOfTypes(expectedColumns, TsColumnTimestamp))
}

func TestTimeseriesColumnsReturnsAliasNotFound(t *testing.T) {
	handle := newTestHandle(t)
	entry := handle.Timeseries(generateAlias(16))

	blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns, err := entry.Columns()
	if !errors.Is(err, ErrAliasNotFound) {
		t.Fatalf("expected ErrAliasNotFound, got err=%v", err)
	}
	if blobColumns != nil || doubleColumns != nil || int64Columns != nil || stringColumns != nil || timestampColumns != nil {
		t.Fatalf(
			"expected nil column slices on error, got blob=%v double=%v int64=%v string=%v timestamp=%v",
			blobColumns,
			doubleColumns,
			int64Columns,
			stringColumns,
			timestampColumns,
		)
	}
}

func newTestTimeseriesMetadataTable(t *testing.T) (TimeseriesEntry, []TsColumnInfo) {
	t.Helper()

	handle := newTestHandle(t)
	entry := handle.Timeseries(generateAlias(16))
	symtable := generateAlias(16)
	columns := []TsColumnInfo{
		NewTsColumnInfo("blob_col", TsColumnBlob),
		NewTsColumnInfo("double_col", TsColumnDouble),
		NewTsColumnInfo("int64_col", TsColumnInt64),
		NewTsColumnInfo("string_col", TsColumnString),
		NewTsColumnInfo("timestamp_col", TsColumnTimestamp),
		NewSymbolColumnInfo("symbol_col", symtable),
	}

	if err := entry.Create(24*time.Hour, columns...); err != nil {
		t.Fatalf("expected Create to succeed, got %v", err)
	}
	t.Cleanup(func() { _ = entry.Remove() })

	expectedColumns := append([]TsColumnInfo{NewTsColumnInfo(tsTimestampColumnName, TsColumnTimestamp)}, columns...)

	return entry, expectedColumns
}

func columnInfosOfTypes(columns []TsColumnInfo, types ...TsColumnType) []TsColumnInfo {
	matchingColumns := []TsColumnInfo{}
	for _, column := range columns {
		for _, columnType := range types {
			if column.Type() == columnType {
				matchingColumns = append(matchingColumns, column)

				break
			}
		}
	}

	return matchingColumns
}

func assertColumnInfos(t *testing.T, got, want []TsColumnInfo) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected %d columns, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		assertColumnInfo(t, got[i], want[i])
	}
}

type columnMetadata interface {
	Name() string
	Type() TsColumnType
	Symtable() string
}

func assertColumns[T columnMetadata](t *testing.T, got []T, want []TsColumnInfo) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("expected %d columns, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		assertColumnInfo(t, got[i], want[i])
	}
}

func assertColumnInfo(t *testing.T, got columnMetadata, want TsColumnInfo) {
	t.Helper()

	if got.Name() != want.Name() || got.Type() != want.Type() || got.Symtable() != want.Symtable() {
		t.Fatalf(
			"unexpected column: got name=%q type=%v symtable=%q; want name=%q type=%v symtable=%q",
			got.Name(),
			got.Type(),
			got.Symtable(),
			want.Name(),
			want.Type(),
			want.Symtable(),
		)
	}
}
