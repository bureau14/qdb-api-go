package qdb

import (
	"fmt"
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
	// TODO(vianney): Debug timestamps, seems like they don't get to see the database
	Context("Timeseries", func() {
		var (
			timeseries      TimeseriesEntry
			doubleColumn    TsDoubleColumn
			blobColumn      TsBlobColumn
			int64Column     TsInt64Column
			timestampColumn TsTimestampColumn
			columnsInfo     []TsColumnInfo
		)
		BeforeEach(func() {
			columnsInfo = []TsColumnInfo{}
			columnsInfo = append(columnsInfo, NewTsColumnInfo("blob_column", TsColumnBlob), NewTsColumnInfo("double_column", TsColumnDouble), NewTsColumnInfo("int64_column", TsColumnInt64), NewTsColumnInfo("timestamp_column", TsColumnTimestamp))
		})
		JustBeforeEach(func() {
			timeseries = handle.Timeseries(alias)
		})
		AfterEach(func() {
			timeseries.Remove()
		})
		Context("Empty columns info", func() {
			BeforeEach(func() {
				columnsInfo = []TsColumnInfo{}
			})
			It("should create even with empty columns", func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should not work to get columns before creating the time series", func() {
				_, _, _, _, err := timeseries.Columns()
				Expect(err).To(HaveOccurred())
			})
			It("should work with zero columns", func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
				doubleCols, blobCols, int64Cols, timestampCols, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(0).To(Equal(len(doubleCols)))
				Expect(0).To(Equal(len(blobCols)))
				Expect(0).To(Equal(len(int64Cols)))
				Expect(0).To(Equal(len(timestampCols)))
			})
			It("should work with a shard size of 1ms", func() {
				err := timeseries.Create(1*time.Millisecond, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should not work with a shard size inferior to 1ms", func() {
				err := timeseries.Create(100*time.Nanosecond, columnsInfo...)
				Expect(err).To(HaveOccurred())
			})
		})
		Context("Created", func() {
			const (
				count int64 = 8
				start int64 = 0
				end   int64 = count - 1
			)
			var (
				timestamps      []time.Time
				doublePoints    []TsDoublePoint
				blobPoints      []TsBlobPoint
				int64Points     []TsInt64Point
				timestampPoints []TsTimestampPoint
			)
			BeforeEach(func() {
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
			})
			JustBeforeEach(func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
				blobColumn = timeseries.BlobColumn(columnsInfo[0].Name())
				doubleColumn = timeseries.DoubleColumn(columnsInfo[1].Name())
				int64Column = timeseries.Int64Column(columnsInfo[2].Name())
				timestampColumn = timeseries.TimestampColumn(columnsInfo[3].Name())
			})
			It("should have one blob column and one double column", func() {
				doubleCols, blobCols, int64Cols, timestampCols, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(1).To(Equal(len(doubleCols)))
				Expect(1).To(Equal(len(blobCols)))
				Expect(1).To(Equal(len(int64Cols)))
				Expect(1).To(Equal(len(timestampCols)))
				Expect(TsColumnDouble).To(Equal(doubleCols[0].Type()))
				Expect(TsColumnBlob).To(Equal(blobCols[0].Type()))
				Expect(TsColumnInt64).To(Equal(int64Cols[0].Type()))
				Expect(TsColumnTimestamp).To(Equal(timestampCols[0].Type()))
			})
			Context("Insert Columns", func() {
				It("should work to insert new columns", func() {
					newColumns := []TsColumnInfo{NewTsColumnInfo("blob_column_2", TsColumnBlob), NewTsColumnInfo("double_column_2", TsColumnDouble)}
					allColumns := append(columnsInfo, newColumns...)

					err := timeseries.InsertColumns(newColumns...)
					Expect(err).ToNot(HaveOccurred())

					cols, err := timeseries.ColumnsInfo()
					Expect(err).ToNot(HaveOccurred())
					Expect(cols).To(ConsistOf(allColumns))
				})
				It("should not work to insert no columns", func() {
					err := timeseries.InsertColumns()
					Expect(err).To(HaveOccurred())
				})
				It("should not work to insert columns with already existing names", func() {
					err := timeseries.InsertColumns(NewTsColumnInfo("blob_column", TsColumnBlob))
					Expect(err).To(HaveOccurred())
				})
			})
			Context("Another timeseries", func() {
				var (
					anotherTimeseries TimeseriesEntry
				)
				JustBeforeEach(func() {
					doubleColumn.Insert(doublePoints...)
					blobColumn.Insert(blobPoints...)
					int64Column.Insert(int64Points...)
					timestampColumn.Insert(timestampPoints...)
					anotherTimeseries = handle.Timeseries(alias)
				})
				It("should be able to retrieve all columns", func() {
					doubleCols, blobCols, int64Cols, timestampCols, err := anotherTimeseries.Columns()
					Expect(err).ToNot(HaveOccurred())
					Expect(1).To(Equal(len(doubleCols)))
					Expect(1).To(Equal(len(blobCols)))
					Expect(1).To(Equal(len(int64Cols)))
					Expect(1).To(Equal(len(timestampCols)))
					Expect(TsColumnDouble).To(Equal(doubleCols[0].Type()))
					Expect(TsColumnBlob).To(Equal(blobCols[0].Type()))
					Expect(TsColumnInt64).To(Equal(int64Cols[0].Type()))
					Expect(TsColumnTimestamp).To(Equal(timestampCols[0].Type()))
				})
				Context("Columns retrieved", func() {
					var (
						doubleCols    []TsDoubleColumn
						blobCols      []TsBlobColumn
						int64Cols     []TsInt64Column
						timestampCols []TsTimestampColumn
						r             TsRange
					)
					JustBeforeEach(func() {
						var err error
						doubleCols, blobCols, int64Cols, timestampCols, err = anotherTimeseries.Columns()
						Expect(err).ToNot(HaveOccurred())
						r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					})
					It("should retrieve all the doubles", func() {
						points, err := doubleCols[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(len(doublePoints)).To(Equal(len(points)))
						for i := range points {
							Expect(doublePoints[i].Timestamp()).To(Equal(points[i].Timestamp()))
							Expect(doublePoints[i].Content()).To(Equal(points[i].Content()))
						}
					})
					It("should retrieve all the blobs", func() {
						points, err := blobCols[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(len(blobPoints)).To(Equal(len(points)))
						for i := range points {
							Expect(blobPoints[i].Timestamp()).To(Equal(points[i].Timestamp()))
							Expect(blobPoints[i].Content()).To(Equal(points[i].Content()))
						}
					})
					It("should retrieve all the int64s", func() {
						points, err := int64Cols[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(len(int64Points)).To(Equal(len(points)))
						for i := range points {
							Expect(int64Points[i].Timestamp()).To(Equal(points[i].Timestamp()))
							Expect(int64Points[i].Content()).To(Equal(points[i].Content()))
						}
					})
					It("should retrieve all the timestamps", func() {
						points, err := timestampCols[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(len(timestampPoints)).To(Equal(len(points)))
						for i := range points {
							Expect(timestampPoints[i].Timestamp()).To(Equal(points[i].Timestamp()))
							Expect(timestampPoints[i].Content()).To(Equal(points[i].Content()))
						}
					})
					It("should be possible to insert a double", func() {
						err := doubleCols[0].Insert(NewTsDoublePoint(timestamps[start], 3.2))
						Expect(err).ToNot(HaveOccurred())
						points, err := doubleColumn.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						anotherPoints, err := doubleCols[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(points).To(Equal(anotherPoints))
						Expect(len(blobPoints) + 1).To(Equal(len(anotherPoints)))
					})
				})
			})
			Context("Erase Data points", func() {
				JustBeforeEach(func() {
					doubleColumn.Insert(doublePoints...)
					blobColumn.Insert(blobPoints...)
					int64Column.Insert(int64Points...)
					timestampColumn.Insert(timestampPoints...)
				})
				Context("Double", func() {
					It("should not work to erase an empty range", func() {
						erasedCount, err := doubleColumn.EraseRanges()
						Expect(err).To(HaveOccurred())
						Expect(uint64(0)).To(BeNumerically("==", erasedCount))
					})
					It("should work to erase a point", func() {
						r := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
						erasedCount, err := doubleColumn.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(1).To(BeNumerically("==", erasedCount))

						rAll := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						doubles, err := doubleColumn.GetRanges(rAll)
						Expect(err).ToNot(HaveOccurred())
						Expect(doublePoints[1:]).To(ConsistOf(doubles))
					})
					It("should work to erase a complete range", func() {
						r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						erasedCount, err := doubleColumn.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(count).To(BeNumerically("==", erasedCount))

						doubles, err := doubleColumn.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(0).To(Equal(len(doubles)))
					})
				})
				Context("Blob", func() {
					It("should not work to erase an empty range", func() {
						erasedCount, err := blobColumn.EraseRanges()
						Expect(err).To(HaveOccurred())
						Expect(uint64(0)).To(Equal(erasedCount))
					})
					It("should work to erase a point", func() {
						r := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
						erasedCount, err := blobColumn.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(1).To(BeNumerically("==", erasedCount))

						rAll := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						blobs, err := blobColumn.GetRanges(rAll)
						Expect(err).ToNot(HaveOccurred())
						Expect(blobPoints[1:]).To(ConsistOf(blobs))
					})
					It("should work to erase a complete range", func() {
						r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						erasedCount, err := blobColumn.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(count).To(BeNumerically("==", erasedCount))

						blobs, err := blobColumn.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(0).To(Equal(len(blobs)))
					})
				})
				Context("Int64", func() {
					It("should not work to erase an empty range", func() {
						erasedCount, err := int64Column.EraseRanges()
						Expect(err).To(HaveOccurred())
						Expect(uint64(0)).To(Equal(erasedCount))
					})
					It("should work to erase a point", func() {
						r := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
						erasedCount, err := int64Column.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(1).To(BeNumerically("==", erasedCount))

						rAll := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						int64Results, err := int64Column.GetRanges(rAll)
						Expect(err).ToNot(HaveOccurred())
						Expect(int64Points[1:]).To(ConsistOf(int64Results))
					})
					It("should work to erase a complete range", func() {
						r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						erasedCount, err := int64Column.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(count).To(BeNumerically("==", erasedCount))

						int64Results, err := int64Column.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(0).To(Equal(len(int64Results)))
					})
				})
				Context("Timestamp", func() {
					It("should not work to erase an empty range", func() {
						erasedCount, err := timestampColumn.EraseRanges()
						Expect(err).To(HaveOccurred())
						Expect(uint64(0)).To(Equal(erasedCount))
					})
					It("should work to erase a point", func() {
						r := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
						erasedCount, err := timestampColumn.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(1).To(BeNumerically("==", erasedCount))

						rAll := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						timestampResults, err := timestampColumn.GetRanges(rAll)
						Expect(err).ToNot(HaveOccurred())
						Expect(timestampPoints[1:]).To(ConsistOf(timestampResults))
					})
					It("should work to erase a complete range", func() {
						r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
						erasedCount, err := timestampColumn.EraseRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(count).To(BeNumerically("==", erasedCount))

						timestampResults, err := timestampColumn.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						Expect(0).To(Equal(len(timestampResults)))
					})
				})
			})
			Context("Insert Data Points", func() {
				It("should work to insert a double point", func() {
					err := doubleColumn.Insert(NewTsDoublePoint(time.Now(), 3.2))
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert a blob point", func() {
					err := blobColumn.Insert(NewTsBlobPoint(time.Now(), []byte("asd")))
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert a int64 point", func() {
					err := int64Column.Insert(NewTsInt64Point(time.Now(), 2))
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert a timestamp point", func() {
					err := timestampColumn.Insert(NewTsTimestampPoint(time.Now(), time.Now()))
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert double points", func() {
					err := doubleColumn.Insert(doublePoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert blob points", func() {
					err := blobColumn.Insert(blobPoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert int64 points", func() {
					err := int64Column.Insert(int64Points...)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert timestamp points", func() {
					err := timestampColumn.Insert(timestampPoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				Context("Empty Points Array", func() {
					BeforeEach(func() {
						doublePoints = []TsDoublePoint{}
						blobPoints = []TsBlobPoint{}
						int64Points = []TsInt64Point{}
						timestampPoints = []TsTimestampPoint{}
					})
					It("should not work to insert double points", func() {
						err := doubleColumn.Insert(doublePoints...)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert blob points", func() {
						err := blobColumn.Insert(blobPoints...)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert int64 points", func() {
						err := int64Column.Insert(int64Points...)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert timestamp points", func() {
						err := timestampColumn.Insert(timestampPoints...)
						Expect(err).To(HaveOccurred())
					})
				})
			})
			// TODO(vianney): better tests on ranges (at least low, middle high timestamps, count number of results and such)
			Context("Ranges", func() {
				JustBeforeEach(func() {
					err := doubleColumn.Insert(doublePoints...)
					Expect(err).ToNot(HaveOccurred())
					err = blobColumn.Insert(blobPoints...)
					Expect(err).ToNot(HaveOccurred())
					err = int64Column.Insert(int64Points...)
					Expect(err).ToNot(HaveOccurred())
					err = timestampColumn.Insert(timestampPoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should create a range", func() {
					r := NewRange(timestamps[start], timestamps[end])
					Expect(timestamps[start]).To(Equal(r.Begin()))
					Expect(timestamps[end]).To(Equal(r.End()))
				})
				It("should get all double points", func() {
					r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					results, err := doubleColumn.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints).To(ConsistOf(results))
				})
				It("should get all blob points", func() {
					r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					results, err := blobColumn.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints).To(ConsistOf(results))
				})
				It("should get all int64 points", func() {
					r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					results, err := int64Column.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(int64Points).To(ConsistOf(results))
				})
				It("should get all timestamp points", func() {
					r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					results, err := timestampColumn.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(timestampPoints).To(ConsistOf(results))
				})
				It("should get the first and last double points", func() {
					r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
					r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
					points := []TsDoublePoint{doublePoints[start], doublePoints[end]}
					results, err := doubleColumn.GetRanges(r1, r2)
					Expect(err).ToNot(HaveOccurred())
					Expect(points).To(ConsistOf(results))
				})
				It("should get the first and last blob points", func() {
					r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
					r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
					results := []TsBlobPoint{blobPoints[start], blobPoints[end]}
					tsBlobPoints, err := blobColumn.GetRanges(r1, r2)
					Expect(err).ToNot(HaveOccurred())
					Expect(results).To(ConsistOf(tsBlobPoints))
				})
				It("should get the first and last int64 points", func() {
					r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
					r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
					results := []TsInt64Point{int64Points[start], int64Points[end]}
					tsInt64Points, err := int64Column.GetRanges(r1, r2)
					Expect(err).ToNot(HaveOccurred())
					Expect(results).To(ConsistOf(tsInt64Points))
				})
				It("should get the first and last timestamp points", func() {
					r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
					r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
					results := []TsTimestampPoint{timestampPoints[start], timestampPoints[end]}
					tsTimestampPoints, err := timestampColumn.GetRanges(r1, r2)
					Expect(err).ToNot(HaveOccurred())
					Expect(results).To(ConsistOf(tsTimestampPoints))
				})
				Context("Empty", func() {
					It("should not get double ranges", func() {
						results, err := doubleColumn.GetRanges()
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(results))
					})
					It("should not get blob ranges", func() {
						results, err := blobColumn.GetRanges()
						Expect(err).To(HaveOccurred())
						Expect([]TsBlobPoint{}).To(ConsistOf(results))
					})
					It("should not get int64 ranges", func() {
						results, err := int64Column.GetRanges()
						Expect(err).To(HaveOccurred())
						Expect([]TsInt64Point{}).To(ConsistOf(results))
					})
					It("should not get timestamp ranges", func() {
						results, err := timestampColumn.GetRanges()
						Expect(err).To(HaveOccurred())
						Expect([]TsTimestampPoint{}).To(ConsistOf(results))
					})
				})
			})
			Context("Aggregate", func() {
				var r TsRange
				JustBeforeEach(func() {
					doubleColumn.Insert(doublePoints...)
					blobColumn.Insert(blobPoints...)

					r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
				})
				Context("Double", func() {
					var doubleAggs []*TsDoubleAggregation
					JustBeforeEach(func() {
						doubleAggFirst := NewDoubleAggregation(AggFirst, r)
						doubleAggLast := NewDoubleAggregation(AggLast, r)
						doubleAggs = []*TsDoubleAggregation{doubleAggFirst, doubleAggLast}
					})
					It("should create a double aggregation", func() {
						agg := NewDoubleAggregation(AggMin, r)
						Expect(AggMin).To(Equal(agg.Type()))
						Expect(r).To(Equal(agg.Range()))
					})
					It("should not work with empty double aggregations", func() {
						_, err := doubleColumn.Aggregate()
						Expect(err).To(HaveOccurred())
					})
					It("should get first double with 'double aggregation'", func() {
						first := doublePoints[start]
						aggs, err := doubleColumn.Aggregate(NewDoubleAggregation(AggFirst, r))
						Expect(err).ToNot(HaveOccurred())
						Expect(first).To(Equal(aggs[0].Result()))
					})
					It("should get first and last elements in timeseries with 'double aggregates'", func() {
						first := doublePoints[start]
						last := doublePoints[end]
						_, err := doubleColumn.Aggregate(doubleAggs...)
						Expect(err).ToNot(HaveOccurred())

						Expect(1).To(BeNumerically("==", doubleAggs[0].Count()))
						Expect(first).To(Equal(doubleAggs[0].Result()))

						Expect(1).To(BeNumerically("==", doubleAggs[1].Count()))
						Expect(last).To(Equal(doubleAggs[1].Result()))

						if start != end {
							Expect(first).ToNot(Equal(doubleAggs[1].Result()))
						}
					})
					It("should get sum of all the doubles", func() {
						sum := func(pts []TsDoublePoint) (s float64) {
							for _, pt := range pts {
								s += pt.Content()
							}
							return
						}(doublePoints)
						aggs, err := doubleColumn.Aggregate(NewDoubleAggregation(AggSum, r))
						Expect(err).ToNot(HaveOccurred())
						Expect(count).To(Equal(aggs[0].Count()))
						Expect(sum).To(Equal(aggs[0].Result().Content()))
					})
				})
				Context("Blob", func() {
					var blobAggs []*TsBlobAggregation
					JustBeforeEach(func() {
						blobAggFirst := NewBlobAggregation(AggFirst, r)
						blobAggLast := NewBlobAggregation(AggLast, r)
						blobAggs = []*TsBlobAggregation{blobAggFirst, blobAggLast}
					})
					It("should create a blob aggregation", func() {
						agg := NewBlobAggregation(AggMin, r)
						Expect(AggMin).To(Equal(agg.Type()))
						Expect(r).To(Equal(agg.Range()))
					})
					It("should not work with empty blob aggregations", func() {
						_, err := blobColumn.Aggregate()
						Expect(err).To(HaveOccurred())
					})
					It("should get first blob with 'blob aggregation'", func() {
						first := blobPoints[start]
						aggs, err := blobColumn.Aggregate(NewBlobAggregation(AggFirst, r))
						Expect(err).ToNot(HaveOccurred())
						Expect(first).To(Equal(aggs[0].Result()))
					})
					It("should get first and last elements in timeseries with 'blob aggregates'", func() {
						first := blobPoints[start]
						last := blobPoints[end]
						_, err := blobColumn.Aggregate(blobAggs...)
						Expect(err).ToNot(HaveOccurred())

						Expect(1).To(BeNumerically("==", blobAggs[0].Count()))
						Expect(first).To(Equal(blobAggs[0].Result()))

						Expect(1).To(BeNumerically("==", blobAggs[1].Count()))
						Expect(last).To(Equal(blobAggs[1].Result()))

						if start != end {
							Expect(first).ToNot(Equal(blobAggs[1].Result()))
						}
					})
				})
			})
			Context("Bulk", func() {
				Context("Insert", func() {
					var (
						blobValue      []byte    = []byte("content")
						doubleValue    float64   = 3.2
						int64Value     int64     = 2
						timestampValue time.Time = time.Now()
					)
					It("should append all columns", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Blob(blobValue).Double(doubleValue).Int64(int64Value).Timestamp(timestampValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should append columns", func() {
						bulk, err := timeseries.Bulk(columnsInfo...)
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Blob(blobValue).Double(doubleValue).Int64(int64Value).Timestamp(timestampValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should append columns and ignore fields", func() {
						bulk, err := timeseries.Bulk(columnsInfo...)
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Ignore().Double(doubleValue).Int64(int64Value).Timestamp(timestampValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should append columns on part of timeseries", func() {
						bulk, err := timeseries.Bulk(columnsInfo[0])
						Expect(err).ToNot(HaveOccurred())
						for i := int64(0); i < count; i++ {
							err := bulk.Row(time.Now()).Blob(blobValue).Append()
							Expect(err).ToNot(HaveOccurred())
						}
						Expect(count).To(Equal(int64(bulk.RowCount())))
						_, err = bulk.Push()
						Expect(err).ToNot(HaveOccurred())
						bulk.Release()
					})
					It("should fail to append columns - too much values", func() {
						bulk, err := timeseries.Bulk(columnsInfo...)
						Expect(err).ToNot(HaveOccurred())
						err = bulk.Row(time.Now()).Blob(blobValue).Double(doubleValue).Double(doubleValue).Append()
						Expect(err).To(HaveOccurred())
						bulk.Release()
					})
					It("should fail to append columns - additional column does not exist", func() {
						columnsInfo = append(columnsInfo, NewTsColumnInfo("asd", TsColumnDouble))
						_, err := timeseries.Bulk(columnsInfo...)
						Expect(err).To(HaveOccurred())
					})
					It("should fail to retrieve all columns from a non existing timeseries", func() {
						ts := handle.Timeseries("asd")
						_, err := ts.Bulk()
						Expect(err).To(HaveOccurred())
					})
				})
				Context("Get", func() {
					var (
						r TsRange
					)
					BeforeEach(func() {
						r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					})
					JustBeforeEach(func() {
						blobColumn.Insert(blobPoints...)
						doubleColumn.Insert(doublePoints...)
						int64Column.Insert(int64Points...)
						timestampColumn.Insert(timestampPoints...)
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

							blobValue, err := bulk.GetBlob()
							Expect(err).ToNot(HaveOccurred())
							Expect(blobPoints[bulk.RowCount()].Content()).To(Equal(blobValue))

							doubleValue, err := bulk.GetDouble()
							Expect(err).ToNot(HaveOccurred())
							Expect(doublePoints[bulk.RowCount()].Content()).To(Equal(doubleValue))

							int64Value, err := bulk.GetInt64()
							Expect(err).ToNot(HaveOccurred())
							Expect(int64Points[bulk.RowCount()].Content()).To(Equal(int64Value))

							timestampValue, err := bulk.GetTimestamp()
							Expect(err).ToNot(HaveOccurred())
							Expect(timestampPoints[bulk.RowCount()].Content()).To(Equal(timestampValue))
						}
						Expect(err).To(Equal(ErrIteratorEnd))
						bulk.Release()
					})
					It("Should not work to get values when GetRanges has not been called", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())

						_, err = bulk.NextRow()
						Expect(err).To(HaveOccurred())
					})
					It("Should not work to get values in a non-correct order", func() {
						bulk, err := timeseries.Bulk()
						Expect(err).ToNot(HaveOccurred())

						err = bulk.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())

						_, err = bulk.NextRow()
						Expect(err).ToNot(HaveOccurred())

						_, err = bulk.GetDouble()
						Expect(err).To(HaveOccurred())
					})
				})
			})

			Context("Batch", func() {
				var (
					batchColumnsInfos []TsBatchColumnInfo
				)
				BeforeEach(func() {
					batchColumnsInfos = make([]TsBatchColumnInfo, 0)
					for _, col := range columnsInfo {
						batchColumnsInfos = append(batchColumnsInfos, TsBatchColumnInfo{alias, col.name, 10})
					}
				})
				Context("Init", func() {
					It("should not work with no columns", func() {
						_, err := handle.TsBatch()
						Expect(err).To(HaveOccurred())
					})
					It("should not work with a bad column", func() {
						newBatchColumnsInfos := make([]TsBatchColumnInfo, len(batchColumnsInfos))
						copy(newBatchColumnsInfos, batchColumnsInfos)
						newBatchColumnsInfos[0].Timeseries = "alias"
						_, err := handle.TsBatch(newBatchColumnsInfos...)
						Expect(err).To(HaveOccurred())
					})
				})
				Context("Initialized", func() {
					var (
						tsBatch *TsBatch
					)
					JustBeforeEach(func() {
						var err error
						tsBatch, err = handle.TsBatch(batchColumnsInfos...)
						Expect(err).ToNot(HaveOccurred())
					})
					AfterEach(func() {
						tsBatch.Release()
					})
					Context("Row", func() {
						var (
							blobValue      []byte    = []byte("content")
							doubleValue    float64   = 3.2
							int64Value     int64     = 2
							timestampValue time.Time = time.Now()
						)
						It("should append all columns", func() {
							err := tsBatch.RowSetBlob(blobValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetDouble(doubleValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetInt64(int64Value)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetTimestamp(timestampValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowFinalize(timestampValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should append columns and ignore fields", func() {
							err := tsBatch.RowSetBlob(blobValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetDouble(doubleValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetInt64(int64Value)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSkipColumn()
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowFinalize(timestampValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should append columns on part of timeseries", func() {
							err := tsBatch.RowSetBlob(blobValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowFinalize(timestampValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should fail to append columns - too much values", func() {
							err := tsBatch.RowSetBlob(blobValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetDouble(doubleValue)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSetInt64(int64Value)
							Expect(err).ToNot(HaveOccurred())
							err = tsBatch.RowSkipColumn()
							Expect(err).ToNot(HaveOccurred())

							err = tsBatch.RowSetInt64(int64Value)
							Expect(err).To(HaveOccurred())

							err = tsBatch.RowFinalize(timestampValue)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should fail to append columns - wrong column type", func() {
							err := tsBatch.RowSetInt64(int64Value)
							Expect(err).To(HaveOccurred())
						})
						Context("Push", func() {
							JustBeforeEach(func() {
								err := tsBatch.RowSetBlob(blobValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetDouble(doubleValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetInt64(int64Value)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowSetTimestamp(timestampValue)
								Expect(err).ToNot(HaveOccurred())
								err = tsBatch.RowFinalize(timestampValue)
								Expect(err).ToNot(HaveOccurred())
							})
							It("should push", func() {
								err := tsBatch.Push()
								Expect(err).ToNot(HaveOccurred())

								rg := NewRange(timestampValue, timestampValue.Add(5*time.Nanosecond))

								blobs, err := blobColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(blobs)).To(Equal(1))
								Expect(blobs[0].Content()).To(Equal(blobValue))

								doubles, err := doubleColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(doubles)).To(Equal(1))
								Expect(doubles[0].Content()).To(Equal(doubleValue))

								int64s, err := int64Column.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(int64s)).To(Equal(1))
								Expect(int64s[0].Content()).To(Equal(int64Value))

								nTimestamps, err := timestampColumn.GetRanges(rg)
								Expect(err).ToNot(HaveOccurred())
								Expect(len(nTimestamps)).To(Equal(1))
								Expect(nTimestamps[0].Content().Equal(timestampValue)).To(BeTrue())
							})
						})
					})
				})
			})
		})
	})
})
