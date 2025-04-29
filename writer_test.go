package qdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriterTableCreateNew(t *testing.T) {
	assert := assert.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	writerTable, err := NewWriterTable(tableName, columns)

	if assert.Nil(err) && assert.NotNil(writerTable) {
		assert.Equal(tableName, writerTable.GetName(), "table names should be equal")
	}
}

func TestWriterTableCanSetIndex(t *testing.T) {
	assert := assert.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	table, err := NewWriterTable(tableName, columns)
	assert.Nil(err)

	idx := generateDefaultIndex(1024)
	err = table.SetIndex(TimeSliceToQdbTimespec(idx))
	assert.Nil(err)
}

func TestWriterTableCanSetDataAllColumnNames(t *testing.T) {
	assert := assert.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumnsOfAllTypes()

	table, err := NewWriterTable(tableName, columns)
	assert.Nil(err)

	idx := generateDefaultIndex(1024)
	err = table.SetIndex(TimeSliceToQdbTimespec(idx))
	assert.Nil(err)

	datas := generateWriterDatas(len(*idx), columns)
	err = table.SetDatas(datas)

	if assert.Nil(err) {
		for i, inData := range datas {
			outData, err := table.GetData(i)
			if assert.Nil(err) {
				assert.Equal(inData, outData, "expect data arrays to be identical")
			}

		}
	}

}
