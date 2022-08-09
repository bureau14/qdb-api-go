package qdb

import (
	"fmt"
	"time"
	"unsafe"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		alias string

		timeseries TimeseriesEntry

		blobColumn      TsBlobColumn
		doubleColumn    TsDoubleColumn
		int64Column     TsInt64Column
		stringColumn    TsStringColumn
		timestampColumn TsTimestampColumn
		symbolColumn    TsStringColumn
		columnsInfo     []TsColumnInfo

		timestamps      []time.Time
		blobPoints      []TsBlobPoint
		doublePoints    []TsDoublePoint
		int64Points     []TsInt64Point
		stringPoints    []TsStringPoint
		timestampPoints []TsTimestampPoint
		symbolPoints    []TsStringPoint
	)
	const (
		count int64 = 8
		start int64 = 0
		end   int64 = count - 1

		blobIndex      int64 = 0
		doubleIndex    int64 = 1
		int64Index     int64 = 2
		stringIndex    int64 = 3
		timestampIndex int64 = 4
		symbolIndex    int64 = 5

		blobColName      string = "blob_col"
		stringColName    string = "string_col"
		doubleColName    string = "double_col"
		int64ColName     string = "int64_col"
		timestampColName string = "timestamp_col"
		symbolColName    string = "symbol_col"

		symtableName string = "symbol_table"
	)
	BeforeEach(func() {
		alias = generateAlias(16)

		columnsInfo = []TsColumnInfo{}
		columnsInfo = append(columnsInfo, NewTsColumnInfo(blobColName, TsColumnBlob), NewTsColumnInfo(doubleColName, TsColumnDouble), NewTsColumnInfo(int64ColName, TsColumnInt64), NewTsColumnInfo(stringColName, TsColumnString), NewTsColumnInfo(timestampColName, TsColumnTimestamp), NewSymbolColumnInfo(symbolColName, symtableName))

		timeseries = handle.Timeseries(alias)

		timestamps = make([]time.Time, count)
		blobPoints = make([]TsBlobPoint, count)
		doublePoints = make([]TsDoublePoint, count)
		int64Points = make([]TsInt64Point, count)
		stringPoints = make([]TsStringPoint, count)
		timestampPoints = make([]TsTimestampPoint, count)
		symbolPoints = make([]TsStringPoint, count)
		for idx := int64(0); idx < count; idx++ {
			timestamps[idx] = time.Unix((idx+1)*10, 0)
			blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
			doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
			int64Points[idx] = NewTsInt64Point(timestamps[idx], idx)
			stringPoints[idx] = NewTsStringPoint(timestamps[idx], fmt.Sprintf("content_%d", idx))
			timestampPoints[idx] = NewTsTimestampPoint(timestamps[idx], timestamps[idx])
			symbolPoints[idx] = NewTsStringPoint(timestamps[idx], fmt.Sprintf("content_%d", idx))
		}

		err := timeseries.Create(24*time.Hour, columnsInfo...)
		Expect(err).ToNot(HaveOccurred())
		blobColumn = timeseries.BlobColumn(columnsInfo[blobIndex].Name())
		doubleColumn = timeseries.DoubleColumn(columnsInfo[doubleIndex].Name())
		int64Column = timeseries.Int64Column(columnsInfo[int64Index].Name())
		stringColumn = timeseries.StringColumn(columnsInfo[stringIndex].Name())
		timestampColumn = timeseries.TimestampColumn(columnsInfo[timestampIndex].Name())
		symbolColumn = timeseries.SymbolColumn(columnsInfo[symbolIndex].Name(), symtableName)

		blobColumn.Insert(blobPoints...)
		doubleColumn.Insert(doublePoints...)
		int64Column.Insert(int64Points...)
		stringColumn.Insert(stringPoints...)
		timestampColumn.Insert(timestampPoints...)
		symbolColumn.Insert(symbolPoints...)
	})
	AfterEach(func() {
		timeseries.Remove()
	})
	Context("Query", func() {
		It("should work", func() {
			fmt.Fprintf(GinkgoWriter, "Meh %s", alias)
			query := fmt.Sprintf("select * from %s in range(1970, +10d)", alias)
			q := handle.Query(query)
			result, err := q.Execute()
			defer handle.Release(unsafe.Pointer(result))
			Expect(err).ToNot(HaveOccurred())

			fmt.Fprintf(GinkgoWriter, "err: %v", err)

			for rowIdx, row := range result.Rows() {
				columns := result.Columns(row)
				// first column is the timestamps of the row
				// second column is the table name
				// values begin at 2
				blobValue, err := columns[blobIndex+2].GetBlob()
				Expect(err).ToNot(HaveOccurred())
				Expect(blobValue).To(Equal(blobPoints[rowIdx].Content()))

				doubleValue, err := columns[doubleIndex+2].GetDouble()
				Expect(err).ToNot(HaveOccurred())
				Expect(doubleValue).To(Equal(doublePoints[rowIdx].Content()))

				int64Value, err := columns[int64Index+2].GetInt64()
				Expect(err).ToNot(HaveOccurred())
				Expect(int64Value).To(Equal(int64Points[rowIdx].Content()))

				stringValue, err := columns[stringIndex+2].GetString()
				Expect(err).ToNot(HaveOccurred())
				Expect(stringValue).To(Equal(stringPoints[rowIdx].Content()))

				timestampValue, err := columns[timestampIndex+2].GetTimestamp()
				Expect(err).ToNot(HaveOccurred())
				Expect(timestampValue).To(Equal(timestampPoints[rowIdx].Content()))

				symbolValue, err := columns[symbolIndex+2].GetString()
				Expect(err).ToNot(HaveOccurred())
				Expect(symbolValue).To(Equal(symbolPoints[rowIdx].Content()))

				for i, column := range result.Columns(row) {
					if i == 1 {
						// Skip $table
						continue
					}
					// get values with universal getter
					point := column.Get()
					switch point.Type() {
					case QueryResultBlob:
						value := point.Value()
						Expect(err).ToNot(HaveOccurred())
						Expect(value).To(Equal(blobPoints[rowIdx].Content()))
					case QueryResultDouble:
						value := point.Value()
						Expect(err).ToNot(HaveOccurred())
						Expect(value).To(Equal(doublePoints[rowIdx].Content()))
					case QueryResultInt64:
						value := point.Value()
						Expect(err).ToNot(HaveOccurred())
						Expect(value).To(Equal(int64Points[rowIdx].Content()))
					case QueryResultString:
						value := point.Value()
						Expect(err).ToNot(HaveOccurred())
						Expect(value).To(Equal(stringPoints[rowIdx].Content()))
					case QueryResultTimestamp:
						value := point.Value()
						Expect(err).ToNot(HaveOccurred())
						Expect(value).To(Equal(timestampPoints[rowIdx].Content()))
					}
				}
			}
		})
		It("should not work to do a wrong query", func() {
			query := fmt.Sprintf("select")
			q := handle.Query(query)
			_, err := q.Execute()
			Expect(err).To(HaveOccurred())
		})

		It("should not work to do get the wrong type for a value", func() {
			query := fmt.Sprintf("select * from %s in range(1970, +10d)", alias)
			q := handle.Query(query)
			result, err := q.Execute()
			defer handle.Release(unsafe.Pointer(result))
			Expect(err).ToNot(HaveOccurred())

			for _, row := range result.Rows() {
				columns := result.Columns(row)
				_, err := columns[blobIndex].GetDouble()
				Expect(err).To(HaveOccurred())
			}
		})
		It("should get no results", func() {
			query := fmt.Sprintf("select * from %s in range(1971, +10d)", alias)
			q := handle.Query(query)
			result, err := q.Execute()
			defer handle.Release(unsafe.Pointer(result))
			Expect(err).ToNot(HaveOccurred())
			Expect(result.ScannedPoints()).To(Equal(int64(0)))
			Expect(result.RowCount()).To(Equal(int64(0)))
		})

		It("create table should return 0 results", func() {
			new_alias := generateAlias(16)
			query := fmt.Sprintf("create table %s (stock_id INT64, price DOUBLE)", new_alias)
			q := handle.Query(query)
			result, err := q.Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeNil())
			handle.Query(fmt.Sprintf("drop table %s", new_alias)).Execute()
		})
		It("drop table should return 0 results", func() {
			new_alias := generateAlias(16)
			handle.Query(fmt.Sprintf("create table %s (stock_id INT64, price DOUBLE)", new_alias)).Execute()
			query := fmt.Sprintf("drop table %s", new_alias)
			q := handle.Query(query)
			result, err := q.Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(BeNil())
		})
	})
})
