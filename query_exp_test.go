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
				fmt.Println("table:", table.Name())
				for _, name := range table.ColumnsNames() {
					fmt.Println("\tcolumn:", name)
				}

				for _, row := range table.Rows() {
					for idx, column := range table.Columns(row) {
						if idx == 1 {
							blob, err := column.GetBlob()
							Expect(err).ToNot(HaveOccurred())
							fmt.Println("result:", string(blob))
						}
					}
				}
			}
		})
	})
})
