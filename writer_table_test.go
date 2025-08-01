package qdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeWriterTables(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		result, err := MergeWriterTables([]WriterTable{})
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("single table input", func(t *testing.T) {
		table := createTestWriterTable(t, "test_table", 10)
		result, err := MergeWriterTables([]WriterTable{table})
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, table.TableName, result[0].TableName)
		assert.Equal(t, table.rowCount, result[0].rowCount)
	})

	t.Run("multiple tables with different names", func(t *testing.T) {
		table1 := createTestWriterTable(t, "table1", 5)
		table2 := createTestWriterTable(t, "table2", 3)
		table3 := createTestWriterTable(t, "table3", 7)

		result, err := MergeWriterTables([]WriterTable{table1, table2, table3})
		assert.NoError(t, err)
		assert.Len(t, result, 3)

		// Check that all tables are preserved
		tableNames := make(map[string]bool)
		for _, table := range result {
			tableNames[table.TableName] = true
		}
		assert.True(t, tableNames["table1"])
		assert.True(t, tableNames["table2"])
		assert.True(t, tableNames["table3"])
	})

	t.Run("multiple tables with same name", func(t *testing.T) {
		table1 := createTestWriterTable(t, "test_table", 5)
		table2 := createTestWriterTable(t, "test_table", 3)
		table3 := createTestWriterTable(t, "test_table", 7)

		result, err := MergeWriterTables([]WriterTable{table1, table2, table3})
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "test_table", result[0].TableName)
		assert.Equal(t, 15, result[0].rowCount) // 5 + 3 + 7
	})

	t.Run("mixed table names", func(t *testing.T) {
		table1 := createTestWriterTable(t, "table1", 5)
		table2 := createTestWriterTable(t, "table2", 3)
		table3 := createTestWriterTable(t, "table1", 7) // same name as table1
		table4 := createTestWriterTable(t, "table3", 2)

		result, err := MergeWriterTables([]WriterTable{table1, table2, table3, table4})
		assert.NoError(t, err)
		assert.Len(t, result, 3) // table1 merged, table2 alone, table3 alone

		// Check merged table1
		var mergedTable1 WriterTable
		found := false
		for _, table := range result {
			if table.TableName == "table1" {
				mergedTable1 = table
				found = true

				break
			}
		}
		assert.True(t, found)
		assert.Equal(t, 12, mergedTable1.rowCount) // 5 + 7
	})

	t.Run("schema mismatch error", func(t *testing.T) {
		table1 := createTestWriterTable(t, "test_table", 5)
		table2 := createTestWriterTableWithColumns(t, "test_table", 3, []WriterColumn{
			{ColumnName: "different_col", ColumnType: TsColumnInt64},
		})

		result, err := MergeWriterTables([]WriterTable{table1, table2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema mismatch")
		assert.Nil(t, result)
	})
}

func TestMergeSingleTableWriters(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		result, err := MergeSingleTableWriters([]WriterTable{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reason=empty table slice")
		assert.Equal(t, WriterTable{}, result)
	})

	t.Run("single table input", func(t *testing.T) {
		table := createTestWriterTable(t, "test_table", 10)
		result, err := MergeSingleTableWriters([]WriterTable{table})
		assert.NoError(t, err)
		assert.Equal(t, table.TableName, result.TableName)
		assert.Equal(t, table.rowCount, result.rowCount)
	})

	t.Run("multiple tables with same name", func(t *testing.T) {
		table1 := createTestWriterTable(t, "test_table", 5)
		table2 := createTestWriterTable(t, "test_table", 3)
		table3 := createTestWriterTable(t, "test_table", 7)

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2, table3})
		assert.NoError(t, err)
		assert.Equal(t, "test_table", result.TableName)
		assert.Equal(t, 15, result.rowCount) // 5 + 3 + 7
		assert.Equal(t, len(table1.data), len(result.data))
		assert.Len(t, result.idx, 15)
	})

	t.Run("table name mismatch", func(t *testing.T) {
		table1 := createTestWriterTable(t, "table1", 5)
		table2 := createTestWriterTable(t, "table2", 3)

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected=table1, actual=table2")
		assert.Equal(t, WriterTable{}, result)
	})

	t.Run("schema mismatch", func(t *testing.T) {
		table1 := createTestWriterTable(t, "test_table", 5)
		table2 := createTestWriterTableWithColumns(t, "test_table", 3, []WriterColumn{
			{ColumnName: "different_col", ColumnType: TsColumnInt64},
		})

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema mismatch")
		assert.Equal(t, WriterTable{}, result)
	})

	t.Run("merged data validation", func(t *testing.T) {
		// Create tables with known data
		table1 := createTestWriterTableWithData(t, "test_table", []int64{1, 2, 3})
		table2 := createTestWriterTableWithData(t, "test_table", []int64{4, 5})
		table3 := createTestWriterTableWithData(t, "test_table", []int64{6, 7, 8, 9})

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2, table3})
		assert.NoError(t, err)
		assert.Equal(t, 9, result.rowCount)

		// Verify data was merged correctly
		col, err := result.GetData(0)
		assert.NoError(t, err)
		intCol, ok := col.(*ColumnDataInt64)
		assert.True(t, ok)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, intCol.xs)
	})
}

func TestMergeWriterTablesPerformance(t *testing.T) {
	t.Run("large dataset", func(t *testing.T) {
		const tableCount = 100
		const rowsPerTable = 1000

		tables := make([]WriterTable, tableCount)
		for i := range tableCount {
			tables[i] = createTestWriterTable(t, "test_table", rowsPerTable)
		}

		result, err := MergeWriterTables(tables)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, tableCount*rowsPerTable, result[0].rowCount)
	})
}

func TestMergeWriterTablesEdgeCases(t *testing.T) {
	t.Run("different column types", func(t *testing.T) {
		// Test all supported column types
		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
			{ColumnName: "string_col", ColumnType: TsColumnString},
			{ColumnName: "blob_col", ColumnType: TsColumnBlob},
			{ColumnName: "timestamp_col", ColumnType: TsColumnTimestamp},
		}

		table1 := createTestWriterTableWithAllTypes(t, "test_table", 3, cols)
		table2 := createTestWriterTableWithAllTypes(t, "test_table", 2, cols)

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.NoError(t, err)
		assert.Equal(t, 5, result.rowCount)
		assert.Equal(t, len(cols), len(result.data))

		// Verify each column type was merged correctly
		for i, col := range cols {
			data, err := result.GetData(i)
			assert.NoError(t, err)
			assert.Equal(t, col.ColumnType.AsValueType(), data.ValueType())
			assert.Equal(t, 5, data.Length())
		}
	})

	t.Run("nil and empty data validation", func(t *testing.T) {
		// Create table with empty data
		table1 := createTestWriterTableWithEmptyData(t, "test_table", 0)
		table2 := createTestWriterTableWithEmptyData(t, "test_table", 0)

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.NoError(t, err)
		assert.Equal(t, 0, result.rowCount)
		assert.Empty(t, result.idx)

		// Verify column data is empty
		for i := range len(result.data) {
			data, err := result.GetData(i)
			assert.NoError(t, err)
			assert.Equal(t, 0, data.Length())
		}
	})

	t.Run("large data sets with actual validation", func(t *testing.T) {
		const tableCount = 10
		const rowsPerTable = 100

		tables := make([]WriterTable, tableCount)
		expectedIntValues := make([]int64, 0, tableCount*rowsPerTable)

		for i := range tableCount {
			baseValue := int64(i * rowsPerTable)
			values := make([]int64, rowsPerTable)
			for j := range rowsPerTable {
				values[j] = baseValue + int64(j)
				expectedIntValues = append(expectedIntValues, baseValue+int64(j))
			}
			tables[i] = createTestWriterTableWithData(t, "test_table", values)
		}

		result, err := MergeSingleTableWriters(tables)
		assert.NoError(t, err)
		assert.Equal(t, tableCount*rowsPerTable, result.rowCount)

		// Verify actual data values
		col, err := result.GetData(0)
		assert.NoError(t, err)
		intCol, ok := col.(*ColumnDataInt64)
		assert.True(t, ok)
		assert.Equal(t, expectedIntValues, intCol.xs)
	})

	t.Run("column order consistency", func(t *testing.T) {
		originalCols := []WriterColumn{
			{ColumnName: "first_col", ColumnType: TsColumnInt64},
			{ColumnName: "second_col", ColumnType: TsColumnDouble},
			{ColumnName: "third_col", ColumnType: TsColumnString},
		}

		table1 := createTestWriterTableWithColumns(t, "test_table", 3, originalCols)
		table2 := createTestWriterTableWithColumns(t, "test_table", 2, originalCols)

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.NoError(t, err)

		// Verify column order is preserved
		assert.Equal(t, len(originalCols), len(result.columnInfoByOffset))
		for i, expectedCol := range originalCols {
			assert.Equal(t, expectedCol.ColumnName, result.columnInfoByOffset[i].ColumnName)
			assert.Equal(t, expectedCol.ColumnType, result.columnInfoByOffset[i].ColumnType)
		}

		// Verify column name lookup is preserved
		for i, col := range originalCols {
			assert.Equal(t, i, result.columnOffsetByName[col.ColumnName])
		}
	})

	t.Run("index continuity", func(t *testing.T) {
		// Create tables with specific timestamps - use UTC to avoid timezone issues
		table1 := createTestWriterTableWithTimestamps(t, "test_table", []time.Time{
			time.Unix(1000, 0).UTC(),
			time.Unix(2000, 0).UTC(),
			time.Unix(3000, 0).UTC(),
		})
		table2 := createTestWriterTableWithTimestamps(t, "test_table", []time.Time{
			time.Unix(4000, 0).UTC(),
			time.Unix(5000, 0).UTC(),
		})

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.NoError(t, err)
		assert.Equal(t, 5, result.rowCount)

		// Verify timestamp indices are properly concatenated
		resultTimes := result.GetIndex()
		expectedTimes := []time.Time{
			time.Unix(1000, 0).UTC(),
			time.Unix(2000, 0).UTC(),
			time.Unix(3000, 0).UTC(),
			time.Unix(4000, 0).UTC(),
			time.Unix(5000, 0).UTC(),
		}
		assert.Equal(t, expectedTimes, resultTimes)
	})

	t.Run("zero rows edge case", func(t *testing.T) {
		// Create a table with zero rows that has the same schema
		emptyCols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
		}
		table1 := createTestWriterTableWithColumns(t, "test_table", 0, emptyCols)
		table2 := createTestWriterTableWithData(t, "test_table", []int64{100, 200, 300})

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		assert.NoError(t, err)
		assert.Equal(t, 3, result.rowCount)

		// Verify data from non-empty table is preserved
		col, err := result.GetData(0)
		assert.NoError(t, err)
		intCol, ok := col.(*ColumnDataInt64)
		assert.True(t, ok)
		assert.Equal(t, []int64{100, 200, 300}, intCol.xs)
	})

	t.Run("mixed empty and non-empty tables", func(t *testing.T) {
		// All tables must have the same schema - use consistent column layout
		emptyCols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
		}

		table1 := createTestWriterTableWithData(t, "test_table", []int64{1, 2})
		table2 := createTestWriterTableWithColumns(t, "test_table", 0, emptyCols)
		table3 := createTestWriterTableWithData(t, "test_table", []int64{3, 4, 5})
		table4 := createTestWriterTableWithColumns(t, "test_table", 0, emptyCols)

		result, err := MergeSingleTableWriters([]WriterTable{table1, table2, table3, table4})
		assert.NoError(t, err)
		assert.Equal(t, 5, result.rowCount)

		// Verify data from non-empty tables is properly merged
		col, err := result.GetData(0)
		assert.NoError(t, err)
		intCol, ok := col.(*ColumnDataInt64)
		assert.True(t, ok)
		assert.Equal(t, []int64{1, 2, 3, 4, 5}, intCol.xs)
	})
}

// Helper functions for testing

func createTestWriterTable(t *testing.T, name string, rowCount int) WriterTable {
	cols := []WriterColumn{
		{ColumnName: "int_col", ColumnType: TsColumnInt64},
		{ColumnName: "double_col", ColumnType: TsColumnDouble},
		{ColumnName: "string_col", ColumnType: TsColumnString},
	}

	return createTestWriterTableWithColumns(t, name, rowCount, cols)
}

func createTestWriterTableWithColumns(t *testing.T, name string, rowCount int, cols []WriterColumn) WriterTable {
	table, err := NewWriterTable(name, cols)
	require.NoError(t, err)

	// Create test data
	timestamps := make([]time.Time, rowCount)
	for i := range rowCount {
		timestamps[i] = time.Unix(int64(i), 0)
	}
	table.SetIndex(timestamps)

	// Create column data
	for i, col := range cols {
		var data ColumnData
		switch col.ColumnType {
		case TsColumnInt64:
			intSlice := make([]int64, rowCount)
			for j := range rowCount {
				intSlice[j] = int64(j)
			}
			intData := NewColumnDataInt64(intSlice)
			data = &intData
		case TsColumnDouble:
			doubleSlice := make([]float64, rowCount)
			for j := range rowCount {
				doubleSlice[j] = float64(j)
			}
			doubleData := NewColumnDataDouble(doubleSlice)
			data = &doubleData
		case TsColumnString:
			stringSlice := make([]string, rowCount)
			for j := range rowCount {
				stringSlice[j] = "test" + string(rune(48+j))
			}
			stringData := NewColumnDataString(stringSlice)
			data = &stringData
		}
		err := table.SetData(i, data)
		require.NoError(t, err)
	}

	return table
}

func createTestWriterTableWithData(t *testing.T, name string, intData []int64) WriterTable {
	cols := []WriterColumn{
		{ColumnName: "int_col", ColumnType: TsColumnInt64},
	}

	table, err := NewWriterTable(name, cols)
	require.NoError(t, err)

	rowCount := len(intData)
	timestamps := make([]time.Time, rowCount)
	for i := range rowCount {
		timestamps[i] = time.Unix(int64(i), 0)
	}
	table.SetIndex(timestamps)

	// Set the integer data
	columnData := NewColumnDataInt64(intData)
	err = table.SetData(0, &columnData)
	require.NoError(t, err)

	return table
}

func createTestWriterTableWithAllTypes(t *testing.T, name string, rowCount int, cols []WriterColumn) WriterTable {
	table, err := NewWriterTable(name, cols)
	require.NoError(t, err)

	// Create test data
	timestamps := make([]time.Time, rowCount)
	for i := range rowCount {
		timestamps[i] = time.Unix(int64(i*1000), 0)
	}
	table.SetIndex(timestamps)

	// Create column data for all types
	for i, col := range cols {
		var data ColumnData
		switch col.ColumnType {
		case TsColumnInt64:
			intSlice := make([]int64, rowCount)
			for j := range rowCount {
				intSlice[j] = int64(j + 100)
			}
			intData := NewColumnDataInt64(intSlice)
			data = &intData
		case TsColumnDouble:
			doubleSlice := make([]float64, rowCount)
			for j := range rowCount {
				doubleSlice[j] = float64(j) + 0.5
			}
			doubleData := NewColumnDataDouble(doubleSlice)
			data = &doubleData
		case TsColumnString:
			stringSlice := make([]string, rowCount)
			for j := range rowCount {
				stringSlice[j] = "value_" + string(rune(48+j))
			}
			stringData := NewColumnDataString(stringSlice)
			data = &stringData
		case TsColumnBlob:
			blobSlice := make([][]byte, rowCount)
			for j := range rowCount {
				blobSlice[j] = []byte{byte(j), byte(j + 1), byte(j + 2)}
			}
			blobData := NewColumnDataBlob(blobSlice)
			data = &blobData
		case TsColumnTimestamp:
			timestampSlice := make([]time.Time, rowCount)
			for j := range rowCount {
				timestampSlice[j] = time.Unix(int64(j*2000), 0)
			}
			timestampData := NewColumnDataTimestamp(timestampSlice)
			data = &timestampData
		}
		err := table.SetData(i, data)
		require.NoError(t, err)
	}

	return table
}

func createTestWriterTableWithEmptyData(t *testing.T, name string, expectedRowCount int) WriterTable {
	cols := []WriterColumn{
		{ColumnName: "int_col", ColumnType: TsColumnInt64},
		{ColumnName: "double_col", ColumnType: TsColumnDouble},
		{ColumnName: "string_col", ColumnType: TsColumnString},
	}

	table, err := NewWriterTable(name, cols)
	require.NoError(t, err)

	// Create empty timestamps
	timestamps := make([]time.Time, 0)
	table.SetIndex(timestamps)

	// Always create empty column data structures, even for zero rows
	for i, col := range cols {
		var data ColumnData
		switch col.ColumnType {
		case TsColumnInt64:
			intData := NewColumnDataInt64(nil)
			data = &intData
		case TsColumnDouble:
			doubleData := NewColumnDataDouble(nil)
			data = &doubleData
		case TsColumnString:
			stringData := NewColumnDataString(nil)
			data = &stringData
		}
		err := table.SetData(i, data)
		require.NoError(t, err)
	}

	return table
}

func createTestWriterTableWithTimestamps(t *testing.T, name string, timestamps []time.Time) WriterTable {
	cols := []WriterColumn{
		{ColumnName: "int_col", ColumnType: TsColumnInt64},
	}

	table, err := NewWriterTable(name, cols)
	require.NoError(t, err)

	table.SetIndex(timestamps)

	// Create corresponding integer data
	rowCount := len(timestamps)
	intSlice := make([]int64, rowCount)
	for i := range rowCount {
		intSlice[i] = int64(i + 1000) // offset to make values distinctive
	}

	columnData := NewColumnDataInt64(intSlice)
	err = table.SetData(0, &columnData)
	require.NoError(t, err)

	return table
}

// TestWriterTableSegfaultPrevention tests the main segfault prevention fixes
func TestWriterTableSegfaultPrevention(t *testing.T) {
	t.Run("NewWriterTable initialization prevents nil access", func(t *testing.T) {
		// This test verifies that NewWriterTable initializes all columns with empty data
		// The fix is in lines 46-64 of writer_table.go

		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
			{ColumnName: "string_col", ColumnType: TsColumnString},
			{ColumnName: "blob_col", ColumnType: TsColumnBlob},
			{ColumnName: "timestamp_col", ColumnType: TsColumnTimestamp},
		}

		table, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		// Verify all columns are initialized with non-nil empty data
		for i := range cols {
			data, err := table.GetData(i)
			require.NoError(t, err)
			require.NotNil(t, data, "Column %d should not be nil after NewWriterTable", i)
			require.Equal(t, 0, data.Length(), "Column %d should have empty data", i)
			require.Equal(t, cols[i].ColumnType.AsValueType(), data.ValueType(), "Column %d should have correct type", i)
		}

		// Test that we can safely access all initialized columns
		timestamps := []time.Time{time.Unix(1000, 0)}
		table.SetIndex(timestamps)

		// Should not crash when accessing unset columns during merge
		result, err := MergeSingleTableWriters([]WriterTable{table})
		require.NoError(t, err)
		require.Equal(t, 1, result.rowCount)
	})

	t.Run("partial column data handling", func(t *testing.T) {
		// Create a WriterTable with 3 columns
		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
			{ColumnName: "string_col", ColumnType: TsColumnString},
		}

		table, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		// Set index/timestamps
		timestamps := []time.Time{time.Unix(1000, 0), time.Unix(2000, 0)}
		table.SetIndex(timestamps)

		// Only set data for 2 out of 3 columns
		intData := NewColumnDataInt64([]int64{100, 200})
		err = table.SetData(0, &intData)
		require.NoError(t, err)

		stringData := NewColumnDataString([]string{"test1", "test2"})
		err = table.SetData(2, &stringData)
		require.NoError(t, err)

		// Column 1 (double_col) is left with its initial empty data

		// Try to use the table in a merge operation - this should not crash
		result, err := MergeSingleTableWriters([]WriterTable{table})
		require.NoError(t, err)
		require.Equal(t, table.rowCount, result.rowCount)

		// Verify set columns are preserved
		resultIntCol, err := result.GetData(0)
		require.NoError(t, err)
		intCol, ok := resultIntCol.(*ColumnDataInt64)
		require.True(t, ok)
		require.Equal(t, []int64{100, 200}, intCol.xs)

		resultStringCol, err := result.GetData(2)
		require.NoError(t, err)
		stringCol, ok := resultStringCol.(*ColumnDataString)
		require.True(t, ok)
		require.Equal(t, []string{"test1", "test2"}, stringCol.xs)

		// Verify unset column has empty data (not nil)
		resultDoubleCol, err := result.GetData(1)
		require.NoError(t, err)
		require.NotNil(t, resultDoubleCol)
		require.Equal(t, 0, resultDoubleCol.Length())
	})

	t.Run("empty table merge safety", func(t *testing.T) {
		// Create tables with only empty column data
		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
		}

		table1, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		table2, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		// Set indexes but no column data
		timestamps1 := []time.Time{time.Unix(1000, 0)}
		table1.SetIndex(timestamps1)

		timestamps2 := []time.Time{time.Unix(2000, 0)}
		table2.SetIndex(timestamps2)

		// Attempt merge operations - this should not crash due to safety checks
		result, err := MergeSingleTableWriters([]WriterTable{table1, table2})
		require.NoError(t, err)
		require.Equal(t, 2, result.rowCount)

		// Verify all columns have empty data (not nil)
		for i := range cols {
			resultCol, err := result.GetData(i)
			require.NoError(t, err)
			require.NotNil(t, resultCol)
			require.Equal(t, 0, resultCol.Length())
		}
	})

	t.Run("comprehensive column type initialization", func(t *testing.T) {
		// Test that all supported column types are properly initialized
		// This specifically tests the fix in lines 46-64 of writer_table.go

		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
			{ColumnName: "string_col", ColumnType: TsColumnString},
			{ColumnName: "blob_col", ColumnType: TsColumnBlob},
			{ColumnName: "timestamp_col", ColumnType: TsColumnTimestamp},
		}

		table, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		// Set minimum required data
		timestamps := []time.Time{time.Unix(1000, 0)}
		table.SetIndex(timestamps)

		// Test all column types can be accessed without crashing
		for i, col := range cols {
			data, err := table.GetData(i)
			require.NoError(t, err)
			require.NotNil(t, data, "Column %d (%s) should be initialized", i, col.ColumnName)
			require.Equal(t, 0, data.Length(), "Column %d should start empty", i)
			require.Equal(t, col.ColumnType.AsValueType(), data.ValueType(), "Column %d should have correct type", i)
		}

		// Test that uninitialized columns can be safely merged
		result, err := MergeSingleTableWriters([]WriterTable{table})
		require.NoError(t, err)
		require.Equal(t, 1, result.rowCount)

		// Verify all columns still accessible after merge
		for i := range cols {
			data, err := result.GetData(i)
			require.NoError(t, err)
			require.NotNil(t, data)
			require.Equal(t, 0, data.Length())
		}
	})

	t.Run("merge operations with mixed column data states", func(t *testing.T) {
		// Test the nil checks in MergeSingleTableWriters (lines 403-406, 441-444)
		// by creating tables with different states of column initialization

		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
			{ColumnName: "string_col", ColumnType: TsColumnString},
		}

		// Table 1: Fully populated
		table1, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		timestamps1 := []time.Time{time.Unix(1000, 0), time.Unix(2000, 0)}
		table1.SetIndex(timestamps1)

		intData1 := NewColumnDataInt64([]int64{100, 200})
		err = table1.SetData(0, &intData1)
		require.NoError(t, err)

		doubleData1 := NewColumnDataDouble([]float64{1.0, 2.0})
		err = table1.SetData(1, &doubleData1)
		require.NoError(t, err)

		stringData1 := NewColumnDataString([]string{"a", "b"})
		err = table1.SetData(2, &stringData1)
		require.NoError(t, err)

		// Table 2: Partially populated
		table2, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		timestamps2 := []time.Time{time.Unix(3000, 0)}
		table2.SetIndex(timestamps2)

		intData2 := NewColumnDataInt64([]int64{300})
		err = table2.SetData(0, &intData2)
		require.NoError(t, err)

		// Leave columns 1 and 2 with their default empty initialization

		// Table 3: Only initialized, no data set
		table3, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		timestamps3 := []time.Time{time.Unix(4000, 0)}
		table3.SetIndex(timestamps3)

		// No column data set - all should remain in their initialized empty state

		// Merge all three tables - this tests the nil safety checks
		result, err := MergeSingleTableWriters([]WriterTable{table1, table2, table3})
		require.NoError(t, err)
		require.Equal(t, 4, result.rowCount) // 2 + 1 + 1

		// Verify the merged data for populated columns
		resultIntCol, err := result.GetData(0)
		require.NoError(t, err)
		intCol, ok := resultIntCol.(*ColumnDataInt64)
		require.True(t, ok)
		require.Equal(t, []int64{100, 200, 300}, intCol.xs) // Only populated data is merged

		// Verify partially populated columns work correctly - should not crash
		resultDoubleCol, err := result.GetData(1)
		require.NoError(t, err)
		require.NotNil(t, resultDoubleCol)
		require.Equal(t, 2, resultDoubleCol.Length()) // Only from table1 which had data

		resultStringCol, err := result.GetData(2)
		require.NoError(t, err)
		require.NotNil(t, resultStringCol)
		require.Equal(t, 2, resultStringCol.Length()) // Only from table1 which had data
	})

	t.Run("table structure integrity during operations", func(t *testing.T) {
		// This test ensures that table structure remains sound for toNativeTableData
		// by testing various scenarios that could cause issues

		cols := []WriterColumn{
			{ColumnName: "int_col", ColumnType: TsColumnInt64},
			{ColumnName: "double_col", ColumnType: TsColumnDouble},
		}

		table, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		// Test with minimal data
		timestamps := []time.Time{time.Unix(1000, 0)}
		table.SetIndex(timestamps)

		// Set only one column
		intData := NewColumnDataInt64([]int64{100})
		err = table.SetData(0, &intData)
		require.NoError(t, err)

		// Leave the second column in its initialized empty state

		// This should work without any issues - tests safe access to t.data[i]
		result, err := MergeSingleTableWriters([]WriterTable{table})
		require.NoError(t, err)
		require.Equal(t, 1, result.rowCount)

		// Verify both columns are accessible
		col0, err := result.GetData(0)
		require.NoError(t, err)
		require.NotNil(t, col0)
		require.Equal(t, 1, col0.Length())

		col1, err := result.GetData(1)
		require.NoError(t, err)
		require.NotNil(t, col1)
		require.Equal(t, 0, col1.Length()) // Empty but not nil
	})

	t.Run("stress test with many empty columns", func(t *testing.T) {
		// Create a table with many columns to test that initialization scales properly

		var cols []WriterColumn
		for i := range 20 {
			cols = append(cols, WriterColumn{
				ColumnName: fmt.Sprintf("col_%d", i),
				ColumnType: TsColumnInt64,
			})
		}

		table, err := NewWriterTable("test_table", cols)
		require.NoError(t, err)

		timestamps := []time.Time{time.Unix(1000, 0)}
		table.SetIndex(timestamps)

		// Set data for only every other column
		for i := 0; i < len(cols); i += 2 {
			intData := NewColumnDataInt64([]int64{int64(i * 100)})
			err = table.SetData(i, &intData)
			require.NoError(t, err)
		}

		// Merge should work without issues
		result, err := MergeSingleTableWriters([]WriterTable{table})
		require.NoError(t, err)
		require.Equal(t, 1, result.rowCount)

		// Verify all columns are accessible
		for i := range cols {
			data, err := result.GetData(i)
			require.NoError(t, err)
			require.NotNil(t, data, "Column %d should not be nil", i)

			if i%2 == 0 {
				// Columns we set data for
				require.Equal(t, 1, data.Length(), "Set column %d should have data", i)
			} else {
				// Columns left in initialized empty state
				require.Equal(t, 0, data.Length(), "Empty column %d should have no data", i)
			}
		}
	})
}
