package qdb

import (
	"fmt"
	"time"
	"unsafe"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	const (
		count int64 = 8
		start int64 = 0
		end   int64 = count - 1
	)
	var (
		alias      string
		timeseries TimeseriesEntry

		doubleColumn    TsDoubleColumn
		blobColumn      TsBlobColumn
		int64Column     TsInt64Column
		timestampColumn TsTimestampColumn
		columnsInfo     []TsColumnInfo

		timestamps      []time.Time
		doublePoints    []TsDoublePoint
		blobPoints      []TsBlobPoint
		int64Points     []TsInt64Point
		timestampPoints []TsTimestampPoint
	)

	BeforeEach(func() {
		alias = generateAlias(16)
		columnsInfo = []TsColumnInfo{}
		columnsInfo = append(columnsInfo, NewTsColumnInfo("blob_column", TsColumnBlob), NewTsColumnInfo("double_column", TsColumnDouble), NewTsColumnInfo("int64_column", TsColumnInt64), NewTsColumnInfo("timestamp_column", TsColumnTimestamp))
	})
	JustBeforeEach(func() {
		timeseries = handle.Timeseries(alias)

		timestamps = make([]time.Time, count)
		blobPoints = make([]TsBlobPoint, count)
		doublePoints = make([]TsDoublePoint, count)
		int64Points = make([]TsInt64Point, count)
		timestampPoints = make([]TsTimestampPoint, count)
		for idx := int64(0); idx < count; idx++ {
			timestamps[idx] = time.Unix((idx+1)*10, 0)
			blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
			doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
			int64Points[idx] = NewTsInt64Point(timestamps[idx], idx)
			timestampPoints[idx] = NewTsTimestampPoint(timestamps[idx], timestamps[idx])
		}

		err := timeseries.Create(24*time.Hour, columnsInfo...)
		Expect(err).ToNot(HaveOccurred())
		blobColumn = timeseries.BlobColumn(columnsInfo[0].Name())
		doubleColumn = timeseries.DoubleColumn(columnsInfo[1].Name())
		int64Column = timeseries.Int64Column(columnsInfo[2].Name())
		timestampColumn = timeseries.TimestampColumn(columnsInfo[3].Name())

		doubleColumn.Insert(doublePoints...)
		blobColumn.Insert(blobPoints...)
		int64Column.Insert(int64Points...)
		timestampColumn.Insert(timestampPoints...)
	})
	AfterEach(func() {
		timeseries.Remove()
	})
	Context("QueryExp", func() {
		It("should work", func() {
			query := fmt.Sprintf("select * from %s in range(1970, +10d)", alias)
			q := handle.QueryExp(query)
			result, err := q.Execute()
			defer handle.Release(unsafe.Pointer(result))
			Expect(err).ToNot(HaveOccurred())
			for _, table := range result.Tables() {
				for rowIdx, row := range table.Rows() {
					columns := table.Columns(row)
					// first column is the timestamps of the row, values begin at one
					blobValue, err := columns[1].GetBlob()
					Expect(err).ToNot(HaveOccurred())
					Expect(blobValue).To(Equal(blobPoints[rowIdx].Content()))

					doubleValue, err := columns[2].GetDouble()
					Expect(err).ToNot(HaveOccurred())
					Expect(doubleValue).To(Equal(doublePoints[rowIdx].Content()))

					int64Value, err := columns[3].GetInt64()
					Expect(err).ToNot(HaveOccurred())
					Expect(int64Value).To(Equal(int64Points[rowIdx].Content()))

					timestampValue, err := columns[4].GetTimestamp()
					Expect(err).ToNot(HaveOccurred())
					Expect(timestampValue).To(Equal(timestampPoints[rowIdx].Content()))

					for _, column := range table.Columns(row) {
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
						case QueryResultTimestamp:
							value := point.Value()
							Expect(err).ToNot(HaveOccurred())
							Expect(value).To(Equal(timestampPoints[rowIdx].Content()))
						}
					}
				}
			}
		})
		It("should not work to do a wrong query", func() {
			query := fmt.Sprintf("select")
			q := handle.QueryExp(query)
			_, err := q.Execute()
			Expect(err).To(HaveOccurred())
		})
		It("should not work to do get the wrong type for a value", func() {
			query := fmt.Sprintf("select * from %s in range(1970, +10d)", alias)
			q := handle.QueryExp(query)
			result, err := q.Execute()
			defer handle.Release(unsafe.Pointer(result))
			Expect(err).ToNot(HaveOccurred())
			for _, table := range result.Tables() {
				for _, row := range table.Rows() {
					columns := table.Columns(row)
					_, err := columns[1].GetDouble()
					Expect(err).To(HaveOccurred())
				}
			}
		})
		It("should get no results", func() {
			query := fmt.Sprintf("select * from %s in range(1971, +10d)", alias)
			q := handle.QueryExp(query)
			result, err := q.Execute()
			defer handle.Release(unsafe.Pointer(result))
			Expect(err).ToNot(HaveOccurred())
			Expect(result.ScannedRows()).To(Equal(int64(0)))
			Expect(result.TablesCount()).To(Equal(int64(0)))
		})
		Context("Tricky cases", func() {
			var (
				newTimestamps   []time.Time
				newBlobPoints   []TsBlobPoint
				newDoublePoints []TsDoublePoint
			)
			JustBeforeEach(func() {
				newTimestamps = make([]time.Time, count)
				newBlobPoints = make([]TsBlobPoint, count)
				newDoublePoints = make([]TsDoublePoint, count)
				for idx := 0; idx < int(count); idx++ {
					newTimestamps[idx] = time.Date(1971, 1, 1, 0, 0, (idx+1)*10, 0, time.UTC)
					if (idx % 2) == 0 {
						newBlobPoints[idx] = NewTsBlobPoint(newTimestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
					} else {
						newDoublePoints[idx] = NewTsDoublePoint(newTimestamps[idx], float64(idx))
					}
				}
				doubleColumn.Insert(newDoublePoints...)
				blobColumn.Insert(newBlobPoints...)
			})
			It("should have a none value for each column", func() {
				query := fmt.Sprintf("select * from %s in range(1971, +10d)", alias)
				q := handle.QueryExp(query)
				result, err := q.Execute()
				defer handle.Release(unsafe.Pointer(result))
				Expect(err).ToNot(HaveOccurred())
				Expect(result.TablesCount()).To(Equal(int64(1)))
				for _, table := range result.Tables() {
					for rowIdx, row := range table.Rows() {
						columns := table.Columns(row)
						if (rowIdx % 2) == 0 {
							blobValue, err := columns[1].GetBlob()
							Expect(err).ToNot(HaveOccurred())
							Expect(blobValue).To(Equal(newBlobPoints[rowIdx].Content()))

							doubleValue := columns[2].Get()
							Expect(err).ToNot(HaveOccurred())
							Expect(doubleValue.Type()).To(Equal(QueryResultDouble))
						} else {
							blobValue := columns[1].Get()
							Expect(err).ToNot(HaveOccurred())
							Expect(blobValue.Type()).To(Equal(QueryResultBlob))

							doubleValue, err := columns[2].GetDouble()
							Expect(err).ToNot(HaveOccurred())
							Expect(doubleValue).To(Equal(newDoublePoints[rowIdx].Content()))
						}
					}
				}
			})
		})
	})
})
