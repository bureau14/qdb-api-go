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
