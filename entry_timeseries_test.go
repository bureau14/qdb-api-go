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
			timeseries  TimeseriesEntry
			columnsInfo []TsColumnInfo
		)
		BeforeEach(func() {
			columnsInfo = []TsColumnInfo{}
			columnsInfo = append(columnsInfo, NewTsColumnInfo("blob_column", TsColumnBlob), NewTsColumnInfo("double_column", TsColumnDouble))
		})
		JustBeforeEach(func() {
			timeseries = handle.Timeseries(alias, columnsInfo)
		})
		AfterEach(func() {
			timeseries.Remove()
		})
		Context("Empty columns info", func() {
			BeforeEach(func() {
				columnsInfo = []TsColumnInfo{}
			})
			It("should create even with empty columns", func() {
				err := timeseries.Create()
				Expect(err).ToNot(HaveOccurred())
			})
		})
		Context("Created", func() {
			var (
				count        int = 8
				timestamps   []time.Time
				doublePoints []TsDoublePoint
				blobPoints   []TsBlobPoint
			)
			BeforeEach(func() {
				timestamps = make([]time.Time, count)
				blobPoints = make([]TsBlobPoint, count)
				doublePoints = make([]TsDoublePoint, count)
				for idx := 0; idx < count; idx++ {
					timestamps[idx] = time.Unix(int64((idx+1)*10), 0)
					blobPoints[idx] = NewTsBlobPoint(timestamps[idx], []byte(fmt.Sprintf("content_%d", idx)))
					doublePoints[idx] = NewTsDoublePoint(timestamps[idx], float64(idx))
				}
			})
			JustBeforeEach(func() {
				err := timeseries.Create()
				Expect(err).ToNot(HaveOccurred())
			})
			Context("Insert", func() {
				It("should work to insert blob points", func() {
					err := timeseries.InsertBlob(timeseries.columns[0].Name(), blobPoints)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert double points", func() {
					err := timeseries.InsertDouble(timeseries.columns[1].Name(), doublePoints...)
					Expect(err).ToNot(HaveOccurred())
				})
				Context("Empty Points Array", func() {
					BeforeEach(func() {
						doublePoints = []TsDoublePoint{}
						blobPoints = []TsBlobPoint{}
					})
					It("should not work to insert double points", func() {
						err := timeseries.InsertDouble(timeseries.columns[1].Name(), doublePoints...)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert blob points", func() {
						err := timeseries.InsertBlob(timeseries.columns[0].Name(), blobPoints)
						Expect(err).To(HaveOccurred())
					})
				})
			})
			// TODO(vianney): better tests on ranges (at least low, middle high timestamps, count number of results and such)
			Context("Ranges", func() {
				var (
					ranges []TsRange
				)
				JustBeforeEach(func() {
					timeseries.InsertDouble(timeseries.columns[1].Name(), doublePoints...)
					timeseries.InsertBlob(timeseries.columns[0].Name(), blobPoints)
					r1 := NewRange(time.Unix(0, 0), time.Unix(90, 0))
					ranges = []TsRange{r1}
				})
				It("should get double ranges", func() {
					tsDoublePoints, err := timeseries.GetDoubleRanges(timeseries.columns[1].Name(), ranges...)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints).To(Equal(tsDoublePoints))
				})
				It("should get blob ranges", func() {
					tsBlobPoints, err := timeseries.GetBlobRanges(timeseries.columns[0].Name(), ranges...)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints).To(Equal(tsBlobPoints))
				})
				Context("Empty", func() {
					JustBeforeEach(func() {
						ranges = []TsRange{}
					})
					It("should not get double ranges", func() {
						tsDoublePoints, err := timeseries.GetDoubleRanges(timeseries.columns[1].Name(), ranges...)
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(tsDoublePoints))
					})
					It("should not get blob ranges", func() {
						tsBlobPoints, err := timeseries.GetBlobRanges(timeseries.columns[0].Name(), ranges...)
						Expect(err).To(HaveOccurred())
						Expect([]TsBlobPoint{}).To(ConsistOf(tsBlobPoints))
					})
				})
			})
			Context("Aggregate", func() {
				var (
					doubleAggs []TsDoubleAggregation
					blobAggs   []TsBlobAggregation
					r          TsRange
				)
				JustBeforeEach(func() {
					timeseries.InsertDouble(timeseries.columns[1].Name(), doublePoints...)
					timeseries.InsertBlob(timeseries.columns[0].Name(), blobPoints)
					r = NewRange(time.Unix(0, 0), time.Unix(90, 0))
					doubleAggFirst := NewDoubleAggregation(AggFirst, r)
					doubleAggLast := NewDoubleAggregation(AggLast, r)
					doubleAggs = []TsDoubleAggregation{doubleAggFirst, doubleAggLast}
					blobAggFirst := NewBlobAggregation(AggFirst, r)
					blobAggLast := NewBlobAggregation(AggLast, r)
					blobAggs = []TsBlobAggregation{blobAggFirst, blobAggLast}
				})
				It("should get first double with 'double aggregation'", func() {
					doublePoint, err := timeseries.DoubleAggregate(timeseries.columns[1].Name(), AggFirst, r)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints[0]).To(Equal(doublePoint))
				})
				It("should get first blob with 'blob aggregation'", func() {
					blobPoint, err := timeseries.BlobAggregate(timeseries.columns[0].Name(), AggFirst, r)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints[0]).To(Equal(blobPoint))
				})
				It("should get first and last elements in timeseries with 'double aggregates'", func() {
					err := timeseries.DoubleAggregateBatch(timeseries.columns[1].Name(), &doubleAggs)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints[0]).To(Equal(doubleAggs[0].Result()))
					Expect(doublePoints[count-1]).To(Equal(doubleAggs[1].Result()))
					Expect(doublePoints[2]).ToNot(Equal(doubleAggs[1].Result()))
				})
				It("should get first and last elements in timeseries with 'blob aggregates'", func() {
					err := timeseries.BlobAggregateBatch(timeseries.columns[0].Name(), &blobAggs)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints[0]).To(Equal(blobAggs[0].Result()))
					Expect(blobPoints[count-1]).To(Equal(blobAggs[1].Result()))
					Expect(blobPoints[2]).ToNot(Equal(blobAggs[1].Result()))
				})
				It("should not work with empty double aggregations", func() {
					doubleAggs = []TsDoubleAggregation{}
					err := timeseries.DoubleAggregateBatch(timeseries.columns[1].Name(), &doubleAggs)
					Expect(err).To(HaveOccurred())
				})
				It("should not work with empty double aggregations", func() {
					blobAggs = []TsBlobAggregation{}
					err := timeseries.BlobAggregateBatch(timeseries.columns[0].Name(), &blobAggs)
					Expect(err).To(HaveOccurred())
				})
			})

		})
	})
})
