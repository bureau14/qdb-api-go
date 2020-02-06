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
	Context("Timeseries - Timestamp", func() {
		var (
			timeseries  TimeseriesEntry
			column      TsTimestampColumn
			columnInfo	TsColumnInfo
			timestamps  []time.Time
			points      []TsTimestampPoint
			r           TsRange
			// aggs		[]*TsTimestampAggregation
			value       time.Time
		)
		const (
			count          int64 = 8
			start          int64 = 0
			end            int64 = count - 1
		)
		BeforeEach(func() {
			columnInfo = NewTsColumnInfo("column", TsColumnTimestamp)
			timestamps = make([]time.Time, count)
			points = make([]TsTimestampPoint, count)
			for idx := int64(0); idx < count; idx++ {
				timestamps[idx] = time.Unix((idx+1)*10, 0)
				points[idx] = NewTsTimestampPoint(timestamps[idx], timestamps[idx])
			}
			// aggFirst := NewTimestampAggregation(AggFirst, r)
			// aggLast := NewTimestampAggregation(AggLast, r)
			// aggs = []*TsTimestampAggregation{aggFirst, aggLast}
			value = time.Now()
			r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
			
			timeseries = handle.Timeseries(alias)
			err := timeseries.Create(24*time.Hour, []TsColumnInfo{columnInfo}...)
			Expect(err).ToNot(HaveOccurred())
			column = timeseries.TimestampColumn(columnInfo.Name())
		})
		AfterEach(func() {
			timeseries.Remove()
		})
		It("should have one timestamp column", func() {
			_, _, _, _, cols, err := timeseries.Columns()
			Expect(err).ToNot(HaveOccurred())
			Expect(1).To(Equal(len(cols)))
			Expect(TsColumnTimestamp).To(Equal(cols[0].Type()))
		})
		It("should retrieve one timestamp column", func() {
			_, _, _, _, cols, err := timeseries.Columns()
			Expect(err).ToNot(HaveOccurred())
			Expect(1).To(Equal(len(cols)))
			Expect(column.Type()).To(Equal(cols[0].Type()))
		})
		It("should insert timestamp points", func() {
			column.Insert(points...)
		})
		It("should not return an error when attempting to insert empty array of timestamp points", func() {
			err := column.Insert([]TsTimestampPoint{}...)
			Expect(err).ToNot(HaveOccurred())
		})
		Context("with points inserted", func() {
			BeforeEach(func() {
				column.Insert(points...)
			})
			It("should get all timestamp points", func() {
				r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
				results, err := column.GetRanges(r)
				Expect(err).ToNot(HaveOccurred())
				Expect(results).To(ConsistOf(points))
			})
			It("should get the first and last timestamp points", func() {
				r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
				r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
				pts := []TsTimestampPoint{points[start], points[end]}
				results, err := column.GetRanges(r1, r2)
				Expect(err).ToNot(HaveOccurred())
				Expect(results).To(ConsistOf(pts))
			})
			It("should get empty timestamp points with empty range array", func() {
				results, err := column.GetRanges()
				Expect(err).ToNot(HaveOccurred())
				Expect(results).To(ConsistOf([]TsTimestampPoint{}))
			})
			It("should create a timestamp aggregation", func() {
				Skip("Aggregation is not yet implement")
				// agg := NewTimestampAggregation(AggMin, r)
				// Expect(AggMin).To(Equal(agg.Type()))
				// Expect(r).To(Equal(agg.Range()))
			})
			It("should not work with empty timestamp aggregations", func() {
				Skip("Aggregation is not yet implement")
				// _, err := column.Aggregate()
				// Expect(err).To(HaveOccurred())
			})
			It("should get first timestamp with timestamp aggregation", func() {
				Skip("Aggregation is not yet implement")
				// first := points[start]
				// aggs, err := column.Aggregate(NewTimestampAggregation(AggFirst, r))
				// Expect(err).ToNot(HaveOccurred())
				// Expect(first).To(Equal(aggs[0].Result()))
			})
			It("should get first and last elements in timeseries with timestamp aggregates", func() {
				Skip("Aggregation is not yet implement")
				// first := points[start]
				// last := points[end]
				// _, err := column.Aggregate(aggs...)
				// Expect(err).ToNot(HaveOccurred())
	
				// Expect(1).To(BeNumerically("==", aggs[0].Count()))
				// Expect(first).To(Equal(aggs[0].Result()))
	
				// Expect(1).To(BeNumerically("==", aggs[1].Count()))
				// Expect(last).To(Equal(aggs[1].Result()))
	
				// if start != end {
				// 	Expect(first).ToNot(Equal(aggs[1].Result()))
				// }
			})
			It("should work to erase an empty range", func() {
				erasedCount, err := column.EraseRanges()
				Expect(err).ToNot(HaveOccurred())
				Expect(int64(0)).To(BeNumerically("==", erasedCount))
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
			It("should append timestamp column", func() {
				bulk, err := timeseries.Bulk()
				Expect(err).ToNot(HaveOccurred())
				for i := int64(0); i < count; i++ {
					err := bulk.Row(time.Now()).Timestamp(value).Append()
					Expect(err).ToNot(HaveOccurred())
				}
				_, err = bulk.Push()
				Expect(err).ToNot(HaveOccurred())
				bulk.Release()
			})
			It("should ignore timestamp field", func() {
				bulk, err := timeseries.Bulk([]TsColumnInfo{columnInfo}...)
				Expect(err).ToNot(HaveOccurred())
				for i := int64(0); i < count; i++ {
					err := bulk.Row(time.Now()).Ignore().Append()
					Expect(err).ToNot(HaveOccurred())
				}
				_, err = bulk.Push()
				Expect(err).ToNot(HaveOccurred())
				bulk.Release()
			})
			It("should fail to append columns additional timestamp column that does not exist", func() {
				columnsInfo := []TsColumnInfo{columnInfo, NewTsColumnInfo("asd", TsColumnTimestamp)}
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

					value, err := bulk.GetTimestamp()
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
			It("should append timestamp column", func() {
				err = tsBatch.StartRow(timestamp)
				Expect(err).ToNot(HaveOccurred())
				err = tsBatch.RowSetTimestamp(0, value)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should fail to append any other columns type than timestamp", func() {
				err = tsBatch.StartRow(timestamp)
				Expect(err).ToNot(HaveOccurred())
				err = tsBatch.RowSetBlob(0, []byte("content"))
				Expect(err).To(HaveOccurred())
				err = tsBatch.RowSetDouble(0, float64(1.0))
				Expect(err).To(HaveOccurred())
				err = tsBatch.RowSetInt64(0, int64(1))
				Expect(err).To(HaveOccurred())
			})
			Context("Push", func() {
				JustBeforeEach(func() {
					err = tsBatch.StartRow(timestamp)
					Expect(err).ToNot(HaveOccurred())
					err := tsBatch.RowSetTimestamp(0, value)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should push", func() {
					err = tsBatch.Push()
					Expect(err).ToNot(HaveOccurred())

					results, err := column.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(results)).To(Equal(1))
					Expect(results[0].Content().Equal(value)).To(BeTrue())
				})
			})
			Context("PushFast", func() {
				JustBeforeEach(func() {
					err = tsBatch.StartRow(timestamp)
					Expect(err).ToNot(HaveOccurred())
					err := tsBatch.RowSetTimestamp(0, value)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should push fast", func() {
					err = tsBatch.PushFast()
					Expect(err).ToNot(HaveOccurred())

					results, err := column.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(len(results)).To(Equal(1))
					Expect(results[0].Content().Equal(value)).To(BeTrue())
				})
			})
		})
	})
})