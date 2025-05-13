package qdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterTableCreateNew(t *testing.T) {
	assert := assert.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	writerTable := NewWriterTable(tableName, columns)

	if assert.NotNil(writerTable) {
		assert.Equal(tableName, writerTable.GetName(), "table names should be equal")
	}
}

func TestWriterTableCanSetIndex(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	tableName := generateDefaultAlias()
	columns := generateWriterColumns(1)

	writerTable := NewWriterTable(tableName, columns)
	require.NotNil(writerTable)

	idx := generateDefaultIndex(1024)
	err := writerTable.SetIndex(TimeSliceToQdbTimespec(idx))

	if assert.Nil(err) {
		assert.Equal(QdbTimespecSliceToTime(writerTable.GetIndex()), idx)
	}
}

func TestWriterTableCanSetDataAllColumnNames(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	columns := generateWriterColumnsOfAllTypes()

	handle, err := SetupHandle(insecureURI, 120*time.Second)
	require.Nil(err, fmt.Sprintf("%v", err))
	defer handle.Close()

	table, err := createTableOfWriterColumnsAndDefaultShardSize(handle, columns)
	require.Nil(err, fmt.Sprintf("%v", err))

	writerTable := NewWriterTable(table.alias, columns)
	require.NotNil(writerTable)

	idx := generateDefaultIndex(1024)
	err = writerTable.SetIndex(TimeSliceToQdbTimespec(idx))
	require.Nil(err)

	datas := generateWriterDatas(len(*idx), columns)
	err = writerTable.SetDatas(datas)

	if assert.Nil(err) {
		for i, inData := range datas {
			outData, err := writerTable.GetData(i)
			if assert.Nil(err) {
				assert.Equal(inData, outData, "expect data arrays to be identical")
			}
		}
	}

}
