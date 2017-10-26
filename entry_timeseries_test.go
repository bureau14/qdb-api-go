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
			timeseries   TimeseriesEntry
			doubleColumn TsDoubleColumn
			blobColumn   TsBlobColumn
			columnsInfo  []TsColumnInfo
		)
		BeforeEach(func() {
			columnsInfo = []TsColumnInfo{}
			columnsInfo = append(columnsInfo, NewTsColumnInfo("blob_column", TsColumnBlob), NewTsColumnInfo("double_column", TsColumnDouble))
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
				_, _, err := timeseries.Columns()
				Expect(err).To(HaveOccurred())
			})
			It("should work with zero columns", func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
				doubles, blobs, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(0).To(Equal(len(doubles)))
				Expect(0).To(Equal(len(blobs)))
			})
			It("should work with a shard size of 1ms", func() {
				err := timeseries.Create(1*time.Millisecond, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should not work with a shard size inferior to 1ms", func() {
				err := timeseries.Create(100, columnsInfo...)
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
				timestamps   []time.Time
				doublePoints []TsDoublePoint
				blobPoints   []TsBlobPoint
			)
			BeforeEach(func() {
				timestamps = make([]time.Time, count)
				blobPoints = make([]TsBlobPoint, count)
				doublePoints = make([]TsDoublePoint, count)
				for idx := int64(0); idx < count; idx++ {
					timestamps[idx] = time.Unix((idx+1)*10, 0)
					blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
					doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
				}
			})
			JustBeforeEach(func() {
				err := timeseries.Create(24*time.Hour, columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
				doubleColumn = timeseries.DoubleColumn(columnsInfo[1].Name())
				blobColumn = timeseries.BlobColumn(columnsInfo[0].Name())
			})
			It("should have one blob column and one double column", func() {
				doubles, blobs, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(1).To(Equal(len(doubles)))
				Expect(1).To(Equal(len(blobs)))
				Expect(TsColumnDouble).To(Equal(doubles[0].Type()))
				Expect(TsColumnBlob).To(Equal(blobs[0].Type()))
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
					anotherTimeseries = handle.Timeseries(alias)
				})
				It("should be able to retrieve all columns", func() {
					doubles, blobs, err := anotherTimeseries.Columns()
					Expect(err).ToNot(HaveOccurred())
					Expect(1).To(Equal(len(doubles)))
					Expect(1).To(Equal(len(blobs)))
					Expect(TsColumnDouble).To(Equal(doubles[0].Type()))
					Expect(TsColumnBlob).To(Equal(blobs[0].Type()))
				})
				Context("Columns retrieved", func() {
					var (
						doubles []TsDoubleColumn
						blobs   []TsBlobColumn
						r       TsRange
					)
					JustBeforeEach(func() {
						var err error
						doubles, blobs, err = anotherTimeseries.Columns()
						Expect(err).ToNot(HaveOccurred())
						r = NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					})
					It("should retrieve all the doubles", func() {
						points, err := doubles[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						for i := range doublePoints {
							Expect(doublePoints[i].Timestamp()).To(Equal(points[i].Timestamp()))
							Expect(doublePoints[i].Content()).To(Equal(points[i].Content()))
						}
					})
					It("should retrieve all the blobs", func() {
						points, err := blobs[0].GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						for i := range blobPoints {
							Expect(blobPoints[i].Timestamp()).To(Equal(points[i].Timestamp()))
							Expect(blobPoints[i].Content()).To(Equal(points[i].Content()))
						}
					})
					It("should be possible to insert a double", func() {
						err := doubles[0].Insert(NewTsDoublePoint(timestamps[start], 3.2))
						Expect(err).ToNot(HaveOccurred())
						points, err := doubleColumn.GetRanges(r)
						Expect(err).ToNot(HaveOccurred())
						anotherPoints, err := doubles[0].GetRanges(r)
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
				It("should work to insert double points", func() {
					err := doubleColumn.Insert(doublePoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert blob points", func() {
					err := blobColumn.Insert(blobPoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				Context("Empty Points Array", func() {
					BeforeEach(func() {
						doublePoints = []TsDoublePoint{}
						blobPoints = []TsBlobPoint{}
					})
					It("should not work to insert double points", func() {
						err := doubleColumn.Insert(doublePoints...)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert blob points", func() {
						err := blobColumn.Insert(blobPoints...)
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
				It("should get the first and last points", func() {
					r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
					r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
					points := []TsDoublePoint{doublePoints[start], doublePoints[end]}
					results, err := doubleColumn.GetRanges(r1, r2)
					Expect(err).ToNot(HaveOccurred())
					Expect(points).To(ConsistOf(results))
				})
				It("should get all blob points", func() {
					r := NewRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond))
					results, err := blobColumn.GetRanges(r)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints).To(ConsistOf(results))
				})
				It("should get the first and last points", func() {
					r1 := NewRange(timestamps[start].Truncate(5*time.Nanosecond), timestamps[start].Add(5*time.Nanosecond))
					r2 := NewRange(timestamps[end].Truncate(5*time.Nanosecond), timestamps[end].Add(5*time.Nanosecond))
					results := []TsBlobPoint{blobPoints[start], blobPoints[end]}
					tsBlobPoints, err := blobColumn.GetRanges(r1, r2)
					Expect(err).ToNot(HaveOccurred())
					Expect(results).To(ConsistOf(tsBlobPoints))
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
				})
				Context("Filters", func() {
					It("should get values - FilterUnique - should fail when implementation ready", func() {
						r := NewFilteredRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond), NewFilter().Unique())
						results, err := doubleColumn.GetRanges(r)
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(results))
						// Real result should be - unsure:
						// Expect(doublePoints).To(ConsistOf(results))
					})
					It("should get values - FilterSample - should fail when implementation ready", func() {
						r := NewFilteredRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond), NewFilter().Sample(8))
						results, err := doubleColumn.GetRanges(r)
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(results))
						// Real result should be:
						// Expect(doublePoints).To(ConsistOf(results))
					})
					It("should get all values - FilterDoubleInsideRange - should fail when implementation ready", func() {
						r := NewFilteredRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond), NewFilter().DoubleLimits(0, 100, FilterDoubleInsideRange))
						results, err := doubleColumn.GetRanges(r)
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(results))
						// Real result should be:
						// Expect(doublePoints).To(ConsistOf(results))
					})
					It("should get all values - FilterDoubleOutsideRange - should fail when implementation ready", func() {
						r := NewFilteredRange(timestamps[start], timestamps[end].Add(5*time.Nanosecond), NewFilter().DoubleLimits(100, 200, FilterDoubleOutsideRange))
						results, err := doubleColumn.GetRanges(r)
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(results))
						// Real result should be:
						// Expect(doublePoints).To(ConsistOf(results))
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
				var (
					content []byte
					value   float64
				)
				BeforeEach(func() {
					content = []byte("content")
					value = 3.2
				})
				It("should append all columns", func() {
					bulk, err := timeseries.Bulk()
					Expect(err).ToNot(HaveOccurred())
					for i := int64(0); i < count; i++ {
						err := bulk.Row(time.Now()).Blob(content).Double(value).Append()
						Expect(err).ToNot(HaveOccurred())
					}
					err = bulk.Push()
					Expect(err).ToNot(HaveOccurred())
				})
				It("should append columns", func() {
					bulk, err := timeseries.Bulk(columnsInfo...)
					Expect(err).ToNot(HaveOccurred())
					for i := int64(0); i < count; i++ {
						err := bulk.Row(time.Now()).Blob(content).Double(value).Append()
						Expect(err).ToNot(HaveOccurred())
					}
					err = bulk.Push()
					Expect(err).ToNot(HaveOccurred())
				})
				It("should append columns and ignore fields", func() {
					bulk, err := timeseries.Bulk(columnsInfo...)
					Expect(err).ToNot(HaveOccurred())
					for i := int64(0); i < count; i++ {
						value := 3.2
						err := bulk.Row(time.Now()).Ignore().Double(value).Append()
						Expect(err).ToNot(HaveOccurred())
					}
					err = bulk.Push()
					Expect(err).ToNot(HaveOccurred())
				})
				It("should append columns on part of timeseries", func() {
					bulk, err := timeseries.Bulk(columnsInfo[0])
					Expect(err).ToNot(HaveOccurred())
					for i := int64(0); i < count; i++ {
						content := []byte("content")
						err := bulk.Row(time.Now()).Blob(content).Append()
						Expect(err).ToNot(HaveOccurred())
					}
					Expect(count).To(Equal(int64(bulk.RowCount())))
					err = bulk.Push()
					Expect(err).ToNot(HaveOccurred())
				})
				It("should fail to append columns - too much values", func() {
					bulk, err := timeseries.Bulk(columnsInfo...)
					Expect(err).ToNot(HaveOccurred())
					content := []byte("content")
					value := 3.2
					err = bulk.Row(time.Now()).Blob(content).Double(value).Double(value).Append()
					Expect(err).To(HaveOccurred())
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
		})
	})
})
