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
				doubleContents []float64
				blobContents   [][]byte
				doublePoints   []TsDoublePoint
				blobPoints     []TsBlobPoint
			)
			BeforeEach(func() {
				doubleContents = []float64{}
				doubleContents = append(doubleContents, 3.2, 7.8)
				doublePoints = []TsDoublePoint{}
				for index, doubleContent := range doubleContents {
					doublePoints = append(doublePoints, NewTsDoublePoint(time.Unix(int64((index+1)*10), 0), doubleContent))
				}
				doublePoints = append(doublePoints, NewTsDoublePoint(time.Unix(int64(60), 0), 4.3))
				doublePoints = append(doublePoints, NewTsDoublePoint(time.Unix(int64(80), 0), 4.7))

				blobContents = [][]byte{}
				blobContents = append(blobContents, []byte("content 1"), []byte("content 2"))
				blobPoints = []TsBlobPoint{}
				for index, blobContent := range blobContents {
					blobPoints = append(blobPoints, NewTsBlobPoint(time.Unix(int64((index+1)*10), 0), blobContent))
				}
				blobPoints = append(blobPoints, NewTsBlobPoint(time.Unix(int64(60), 0), []byte("content 3")))
				blobPoints = append(blobPoints, NewTsBlobPoint(time.Unix(int64(80), 0), []byte("content 4")))
			})
			JustBeforeEach(func() {
				err := timeseries.Create()
				Expect(err).ToNot(HaveOccurred())
			})
			Context("Insert", func() {
				It("should work to insert blob points", func() {
					err := timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert double points", func() {
					err := timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
					Expect(err).ToNot(HaveOccurred())
				})
				Context("Empty Points Array", func() {
					BeforeEach(func() {
						doublePoints = []TsDoublePoint{}
						blobPoints = []TsBlobPoint{}
					})
					It("should not work to insert double points", func() {
						err := timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert blob points", func() {
						err := timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
						Expect(err).To(HaveOccurred())
					})
				})
			})
			// TODO(vianney): better tests on ranges (at least low, middle high timestamps, count number of results and such)
			Context("Ranges", func() {
				var (
					ranges TsRanges
				)
				JustBeforeEach(func() {
					timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
					timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
					r1 := TsRange{time.Unix(0, 0), time.Unix(90, 0)}
					ranges = []TsRange{r1}
				})
				It("should get double ranges", func() {
					tsDoublePoints, err := timeseries.GetDoubleRanges(timeseries.columns[1].Name, ranges)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints).To(Equal(tsDoublePoints))
				})
				It("should get blob ranges", func() {
					tsBlobPoints, err := timeseries.GetBlobRanges(timeseries.columns[0].Name, ranges)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints).To(Equal(tsBlobPoints))
				})
				Context("Empty", func() {
					JustBeforeEach(func() {
						ranges = []TsRange{}
					})
					It("should not get double ranges", func() {
						tsDoublePoints, err := timeseries.GetDoubleRanges(timeseries.columns[1].Name, ranges)
						Expect(err).To(HaveOccurred())
						Expect([]TsDoublePoint{}).To(ConsistOf(tsDoublePoints))
					})
					It("should not get blob ranges", func() {
						tsBlobPoints, err := timeseries.GetBlobRanges(timeseries.columns[0].Name, ranges)
						Expect(err).To(HaveOccurred())
						Expect([]TsBlobPoint{}).To(ConsistOf(tsBlobPoints))
					})
				})
			})
			Context("Aggregate", func() {
				var (
					doubleAggs TsDoubleAggregations
					blobAggs   TsBlobAggregations
					r          TsRange
				)
				JustBeforeEach(func() {
					timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
					timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
					r = TsRange{time.Unix(0, 0), time.Unix(90, 0)}
					doubleAggFirst := TsDoubleAggregation{AggFirst, r, 0, TsDoublePoint{}}
					doubleAggLast := TsDoubleAggregation{AggLast, r, 0, TsDoublePoint{}}
					doubleAggs = TsDoubleAggregations{doubleAggFirst, doubleAggLast}
					blobAggFirst := TsBlobAggregation{AggFirst, r, 0, TsBlobPoint{}}
					blobAggLast := TsBlobAggregation{AggLast, r, 0, TsBlobPoint{}}
					blobAggs = TsBlobAggregations{blobAggFirst, blobAggLast}
				})
				It("should get first double with 'double aggregation'", func() {
					doublePoint, err := timeseries.DoubleAggregate(timeseries.columns[1].Name, AggFirst, r)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints[0]).To(Equal(doublePoint))
				})
				It("should get first blob with 'blob aggregation'", func() {
					blobPoint, err := timeseries.BlobAggregate(timeseries.columns[0].Name, AggFirst, r)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints[0]).To(Equal(blobPoint))
				})
				It("should get first and last elements in timeseries with 'double aggregates'", func() {
					err := timeseries.DoubleAggregates(timeseries.columns[1].Name, &doubleAggs)
					Expect(err).ToNot(HaveOccurred())
					Expect(doublePoints[0]).To(Equal(doubleAggs[0].P))
					Expect(doublePoints[3]).To(Equal(doubleAggs[1].P))
					Expect(doublePoints[2]).ToNot(Equal(doubleAggs[1].P))
				})
				It("should get first and last elements in timeseries with 'blob aggregates'", func() {
					err := timeseries.BlobAggregates(timeseries.columns[0].Name, &blobAggs)
					Expect(err).ToNot(HaveOccurred())
					Expect(blobPoints[0]).To(Equal(blobAggs[0].P))
					Expect(blobPoints[3]).To(Equal(blobAggs[1].P))
					Expect(blobPoints[2]).ToNot(Equal(blobAggs[1].P))
				})
				It("should not work with empty double aggregations", func() {
					doubleAggs = TsDoubleAggregations{}
					err := timeseries.DoubleAggregates(timeseries.columns[1].Name, &doubleAggs)
					Expect(err).To(HaveOccurred())
				})
				It("should not work with empty double aggregations", func() {
					blobAggs = TsBlobAggregations{}
					err := timeseries.BlobAggregates(timeseries.columns[0].Name, &blobAggs)
					Expect(err).To(HaveOccurred())
				})
			})

		})
	})
})
