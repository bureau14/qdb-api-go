package qdb

import "testing"

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

func TestColumnsInfoToColumnsClassifiesColumnTypes(t *testing.T) {
	entry := TimeseriesEntry{}
	blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns := columnsInfoToColumns(entry, []TsColumnInfo{
		NewTsColumnInfo("blob", TsColumnBlob),
		NewTsColumnInfo("double", TsColumnDouble),
		NewTsColumnInfo("int64", TsColumnInt64),
		NewTsColumnInfo("string", TsColumnString),
		NewSymbolColumnInfo("symbol", "symbols"),
		NewTsColumnInfo("timestamp", TsColumnTimestamp),
	})

	if len(blobColumns) != 1 {
		t.Fatalf("expected 1 blob column, got %d", len(blobColumns))
	}
	if blobColumns[0].Name() != "blob" || blobColumns[0].Type() != TsColumnBlob {
		t.Fatalf("unexpected blob column: name=%q type=%v", blobColumns[0].Name(), blobColumns[0].Type())
	}

	if len(doubleColumns) != 1 {
		t.Fatalf("expected 1 double column, got %d", len(doubleColumns))
	}
	if doubleColumns[0].Name() != "double" || doubleColumns[0].Type() != TsColumnDouble {
		t.Fatalf("unexpected double column: name=%q type=%v", doubleColumns[0].Name(), doubleColumns[0].Type())
	}

	if len(int64Columns) != 1 {
		t.Fatalf("expected 1 int64 column, got %d", len(int64Columns))
	}
	if int64Columns[0].Name() != "int64" || int64Columns[0].Type() != TsColumnInt64 {
		t.Fatalf("unexpected int64 column: name=%q type=%v", int64Columns[0].Name(), int64Columns[0].Type())
	}

	if len(stringColumns) != 2 {
		t.Fatalf("expected 2 string columns, got %d", len(stringColumns))
	}
	if stringColumns[0].Name() != "string" || stringColumns[0].Type() != TsColumnString {
		t.Fatalf("unexpected string column: name=%q type=%v", stringColumns[0].Name(), stringColumns[0].Type())
	}
	if stringColumns[1].Name() != "symbol" || stringColumns[1].Type() != TsColumnSymbol || stringColumns[1].Symtable() != "symbols" {
		t.Fatalf(
			"unexpected symbol column: name=%q type=%v symtable=%q",
			stringColumns[1].Name(),
			stringColumns[1].Type(),
			stringColumns[1].Symtable(),
		)
	}

	if len(timestampColumns) != 1 {
		t.Fatalf("expected 1 timestamp column, got %d", len(timestampColumns))
	}
	if timestampColumns[0].Name() != "timestamp" || timestampColumns[0].Type() != TsColumnTimestamp {
		t.Fatalf("unexpected timestamp column: name=%q type=%v", timestampColumns[0].Name(), timestampColumns[0].Type())
	}
}

func TestColumnsInfoToColumnsHandlesEmptyInput(t *testing.T) {
	blobColumns, doubleColumns, int64Columns, stringColumns, timestampColumns := columnsInfoToColumns(TimeseriesEntry{}, nil)

	if len(blobColumns) != 0 {
		t.Fatalf("expected no blob columns, got %d", len(blobColumns))
	}
	if len(doubleColumns) != 0 {
		t.Fatalf("expected no double columns, got %d", len(doubleColumns))
	}
	if len(int64Columns) != 0 {
		t.Fatalf("expected no int64 columns, got %d", len(int64Columns))
	}
	if len(stringColumns) != 0 {
		t.Fatalf("expected no string columns, got %d", len(stringColumns))
	}
	if len(timestampColumns) != 0 {
		t.Fatalf("expected no timestamp columns, got %d", len(timestampColumns))
	}
}
