package qdb

import (
	"testing"
)

func TestCanCreateNewWriterTable(t *testing.T) {
	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	_, err := NewWriterTable(tableName, columns)

	if err != nil {
		t.Fatal(err)
	}
}

func TestWriterTableCanSetIndex(t *testing.T) {
	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	table, err := NewWriterTable(tableName, columns)
	if err != nil {
		t.Fatal(err)
	}

	idx := generateDefaultIndex(1024)
	err = table.SetIndex(TimeSliceToQdbTimespec(idx))

	if err != nil {
		t.Fatal(err)
	}
}

func TestWriterTableCanSetDataAllColumnNames(t *testing.T) {
	tableName := generateDefaultAlias()
	columns := generateWriterColumnsOfAllTypes()

	table, err := NewWriterTable(tableName, columns)
	if err != nil {
		t.Fatal(err)
	}

	idx := generateDefaultIndex(1024)
	err = table.SetIndex(TimeSliceToQdbTimespec(idx))

	if err != nil {
		t.Fatal(err)
	}

	datas := generateWriterDatas(len(idx), columns)

	for i, data := range datas {
		err = table.SetData(i, data)

		if err != nil {
			t.Fatal(err)
		}
	}

}
