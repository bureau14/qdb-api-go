package qdb

import (
	"testing"
)

func TestCanCreateNewWriterTable(t *testing.T) {
	tableName := generateAlias(16)
	columnNames := generateColumnNames(4)

	_, err := NewWriterTable(tableName, columnNames)

	if err != nil {
		t.Fatal(err)
	}
}

func TestWriterTableCanSetIndex(t *testing.T) {
	tableName := generateAlias(16)
	columnNames := generateColumnNames(4)

	table, err := NewWriterTable(tableName, columnNames)
	if err != nil {
		t.Fatal(err)
	}

	idx := generateDefaultIndex(1024)
	err = table.SetIndex(idx)

	if err != nil {
		t.Fatal(err)
	}

}
