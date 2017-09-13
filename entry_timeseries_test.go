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
				err := timeseries.Create(columnsInfo...)
				Expect(err).ToNot(HaveOccurred())
			})
			It("should not work to get columns before creating the time series", func() {
				_, _, err := timeseries.Columns()
				Expect(err).To(HaveOccurred())
			})
			It("should have zero columns", func() {
				err := timeseries.Create(columnsInfo...)
				doubles, blobs, err := timeseries.Columns()
				Expect(err).ToNot(HaveOccurred())
				Expect(0).To(Equal(len(doubles)))
				Expect(0).To(Equal(len(blobs)))
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
				err := timeseries.Create(columnsInfo...)
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
		})
	})
})
