package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		alias string
	)

	BeforeEach(func() {
		alias = generateAlias(16)
	})
	
	// :: Timeseries tests ::
	Context("Timeseries - Double", func() {
		var (
			timeseries  TimeseriesEntry
			column      TsDoubleColumn
			columnInfo	TsColumnInfo
			timestamps  []time.Time
			points      []TsDoublePoint
			r           TsRange
			aggs		[]*TsDoubleAggregation
			value       float64
		)
		const (
			count          int64 = 8
			start          int64 = 0
			end            int64 = count - 1
		)
		BeforeEach(func() {
			columnInfo = NewTsColumnInfo("column", TsColumnDouble)
			timestamps = make([]time.Time, count)
			points = make([]TsDoublePoint, count)
			for idx := int64(0); idx < count; idx++ {
				timestamps[idx] = time.Unix((idx+1)*10, 0)
				points[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
			}
			aggFirst := NewDoubleAggregation(AggFirst, r)
			aggLast := NewDoubleAggregation(AggLast, r)
			aggs = []*TsDoubleAggregation{aggFirst, aggLast}
			value = float64(1.0)
			r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
			
			timeseries = handle.Timeseries(alias)
			err := timeseries.Create(24*time.Hour, []TsColumnInfo{columnInfo}...)
			Expect(err).ToNot(HaveOccurred())
			column = timeseries.DoubleColumn(columnInfo.Name())
		})
		AfterEach(func() {
			timeseries.Remove()
		})
		It("should have one double column", func() {
			_, cols, _, _, _, err := timeseries.Columns()
			Expect(err).ToNot(HaveOccurred())
			Expect(1).To(Equal(len(cols)))
			Expect(TsColumnDouble).To(Equal(cols[0].Type()))
		})
		It("should retrieve one double column", func() {
			_, cols, _, _, _, err := timeseries.Columns()
			Expect(err).ToNot(HaveOccurred())
			Expect(1).To(Equal(len(cols)))
			Expect(column.Type()).To(Equal(cols[0].Type()))
		})
		It("should insert double points", func() {
			column.Insert(points...)
		})
		It("should not return an error when attempting to insert empty array of double points", func() {
			err := column.Insert([]TsDoublePoint{}...)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("with points inserted", func() {
			BeforeEach(func() {
				column.Insert(points...)
			})
			It("should get all double points", func() {
				r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
				results, err := column.GetRanges(r)
				Expect(err).ToNot(HaveOccurred())
				Expect(results).To(ConsistOf(points))
			})
			It("should get the first and last double points", func() {
				r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
				r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
				pts := []TsDoublePoint{points[start], points[end]}
				results, err := column.GetRanges(r1, r2)
				Expect(err).ToNot(HaveOccurred())
				Expect(results).To(ConsistOf(pts))
			})
			It("should get empty double points with empty range array", func() {
				results, err := column.GetRanges()
				Expect(err).ToNot(HaveOccurred())
				Expect(results).To(ConsistOf([]TsDoublePoint{}))
			})
			It("should create a double aggregation", func() {
				agg := NewDoubleAggregation(AggMin, r)
				Expect(AggMin).To(Equal(agg.Type()))
				Expect(r).To(Equal(agg.Range()))
			})
			It("should not work with empty double aggregations", func() {
				_, err := column.Aggregate()
				Expect(err).To(HaveOccurred())
			})
			It("should get first double with double aggregation", func() {
				first := points[start]
				aggs, err := column.Aggregate(NewDoubleAggregation(AggFirst, r))
				Expect(err).ToNot(HaveOccurred())
				Expect(first).To(Equal(aggs[0].Result()))
			})
			It("should get first and last elements in timeseries with double aggregates", func() {
				first := points[start]
				last := points[end]
				_, err := column.Aggregate(aggs...)
				Expect(err).ToNot(HaveOccurred())
	
				Expect(count).To(BeNumerically("==", aggs[0].Count()))
				Expect(first).To(Equal(aggs[0].Result()))
	
				Expect(count).To(BeNumerically("==", aggs[1].Count()))
				Expect(last).To(Equal(aggs[1].Result()))
	
				if start != end {
					Expect(first).ToNot(Equal(aggs[1].Result()))
				}
			})
			It("should work to erase an empty range", func() {
				erasedCount, err := column.EraseRanges()
				Expect(err).ToNot(HaveOccurred())
				Expect(uint64(0)).To(BeNumerically("==", erasedCount))
			})
			It("should work to erase a point", func() {
				partialRange := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
				erasedCount, err := column.EraseRanges(partialRange)
				Expect(err).ToNot(HaveOccurred())
				Expect(1).To(BeNumerically("==", erasedCount))

				completeRange := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
				results, err := column.GetRanges(completeRange)
				Expect(err).ToNot(HaveOccurred())
				Expect(points[1:]).To(ConsistOf(results))
			})
			It("should work to erase a complete range", func() {
				completeRange := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
				erasedCount, err := column.EraseRanges(completeRange)
				Expect(err).ToNot(HaveOccurred())
				Expect(count).To(BeNumerically("==", erasedCount))

				results, err := column.GetRanges(completeRange)
				Expect(err).ToNot(HaveOccurred())
				Expect(0).To(Equal(len(results)))
			})
		})
		Context("Bulk", func() {
			It("should fail to append columns additional double column that does not exist", func() {
				columnsInfo := []TsColumnInfo{columnInfo, NewTsColumnInfo("asd", TsColumnDouble)}
				_, err := timeseries.Bulk(columnsInfo...)
				Expect(err).To(HaveOccurred())
			})
			It("Should work to get all values", func() {
				bulk, err := timeseries.Bulk()
				Expect(err).ToNot(HaveOccurred())
				err = bulk.GetRanges(r)
				Expect(err).ToNot(HaveOccurred())
				for {
					var timestamp time.Time
					if timestamp, err = bulk.NextRow(); err != nil {
						break
					}
					Expect(err).ToNot(HaveOccurred())
					Expect(timestamps[bulk.RowCount()]).To(Equal(timestamp))

					value, err := bulk.GetDouble()
					Expect(err).ToNot(HaveOccurred())
					Expect(points[bulk.RowCount()].Content()).To(Equal(value))

				}
				Expect(err).To(Equal(ErrIteratorEnd))
				bulk.Release()
			})
		})
		Context("Batch", func() {
			var (
				tsBatch *TsBatch
				batchColumnsInfos []TsBatchColumnInfo
				err     error
				timestamp time.Time
			)
			BeforeEach(func() {
				batchColumnsInfos = []TsBatchColumnInfo{TsBatchColumnInfo{alias, column.name, 10}}
				timestamp = timestamps[0]
			})
			JustBeforeEach(func() {
				err = nil
				tsBatch, err = handle.TsBatch(batchColumnsInfos...)
				Expect(err).ToNot(HaveOccurred())
			})
			AfterEach(func() {
				tsBatch.Release()
			})
			It("should append double column", func() {
				err = tsBatch.StartRow(timestamp)
				Expect(err).ToNot(HaveOccurred())
				err = tsBatch.RowSetDouble(0, value)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should fail to append any other columns type than double", func() {
				err = tsBatch.StartRow(timestamp)
				Expect(err).ToNot(HaveOccurred())
				err = tsBatch.RowSetBlob(0, []byte("content"))
				Expect(err).To(HaveOccurred())
				err = tsBatch.RowSetInt64(0, int64(1))
				Expect(err).To(HaveOccurred())
				err = tsBatch.RowSetString(0, "content")
				Expect(err).To(HaveOccurred())
				err = tsBatch.RowSetTimestamp(0, time.Now())
				Expect(err).To(HaveOccurred())
			})
			Context("Push", func() {
				JustBeforeEach(func() {
					err = tsBatch.StartRow(timestamp)
					Expect(err).ToNot(HaveOccurred())
					err := tsBatch.RowSetDouble(0, value)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should push", func() {
					err = tsBatch.Push()
					Expect(err).ToNot(HaveOccurred())

					results, err := column.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(results)).To(Equal(1))
					Expect(results[0].Content()).To(Equal(value))
				})
			})
			Context("PushFast", func() {
				JustBeforeEach(func() {
					err = tsBatch.StartRow(timestamp)
					Expect(err).ToNot(HaveOccurred())
					err := tsBatch.RowSetDouble(0, value)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should push fast", func() {
					err = tsBatch.PushFast()
					Expect(err).ToNot(HaveOccurred())

					results, err := column.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(results)).To(Equal(1))
					Expect(results[0].Content()).To(Equal(value))
				})
			})
		})
	})
})