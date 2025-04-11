package qdb

import (
	"testing"
)

func TestCanCreateNewWriterTable(t *testing.T) {
	tableName := generateAlias(16)
	_, err := NewWriterTable(tableName)

	if err != nil {
		t.Fatal(err)
	}
}

func TestWriterTableCanSetIndex(t *testing.T) {
	tableName := generateAlias(16)

	_, err := NewWriterTable(tableName)
	if err != nil {
		t.Fatal(err)
	}
}
