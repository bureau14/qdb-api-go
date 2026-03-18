package qdb

import "testing"

func TestEnsureTimestampColumnFirstAddsMissingColumn(t *testing.T) {
	cols := ensureTimestampColumnFirst(
		NewTsColumnInfo("value", TsColumnDouble),
		NewTsColumnInfo("volume", TsColumnInt64),
	)

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

func TestEnsureTimestampColumnFirstMovesExistingColumn(t *testing.T) {
	cols := ensureTimestampColumnFirst(
		NewTsColumnInfo("value", TsColumnDouble),
		NewTsColumnInfo(tsTimestampColumnName, TsColumnTimestamp),
		NewTsColumnInfo("volume", TsColumnInt64),
	)

	if cols[0].Name() != tsTimestampColumnName {
		t.Fatalf("expected first column to be %q, got %q", tsTimestampColumnName, cols[0].Name())
	}
	if cols[1].Name() != "value" || cols[2].Name() != "volume" {
		t.Fatalf("expected non-timestamp columns to keep relative order, got %+v", cols)
	}
}

func TestEnsureTimestampColumnFirstNormalizesType(t *testing.T) {
	cols := ensureTimestampColumnFirst(
		NewTsColumnInfo(tsTimestampColumnName, TsColumnString),
		NewTsColumnInfo("value", TsColumnDouble),
	)

	if cols[0].Name() != tsTimestampColumnName {
		t.Fatalf("expected first column to be %q, got %q", tsTimestampColumnName, cols[0].Name())
	}
	if cols[0].Type() != TsColumnTimestamp {
		t.Fatalf("expected first column type to be %v, got %v", TsColumnTimestamp, cols[0].Type())
	}
	if cols[1].Name() != "value" {
		t.Fatalf("expected second column to remain %q, got %q", "value", cols[1].Name())
	}
}
