package qdb

import (
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var handle = MustSetupHandle()

// test every entry related files
func TestEntry(t *testing.T) {
	defer handle.Close()
	// Test blobs
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var _ = Describe("Tests", func() {
	var (
		currentHandle HandleType
		alias         string
		err           error
	)
	BeforeSuite(func() {
		currentHandle = MustSetupHandle()
	})

	BeforeEach(func() {
		alias = generateAlias(16)
	})

	// :: Entry tests ::
	Context("Entry", func() {
		var (
			integer IntegerEntry
		)
		JustBeforeEach(func() {
			integer = currentHandle.Integer(alias)
			integer.Put(13, NeverExpires())
		})
		AfterEach(func() {
			integer.Remove()
		})
		// Alias tests
		Context("Alias", func() {
			It("should have alias", func() {
				Expect(alias).To(Equal(integer.Alias()))
			})
			Context("Empty alias", func() {
				BeforeEach(func() {
					alias = ""
				})
				It("should not put", func() {
					err := integer.Put(17, NeverExpires())
					Expect(err).To(HaveOccurred())
				})
			})
		})

		// Tags test
		Context("Tags", func() {
			var (
				tag  string
				tags []string
			)
			BeforeEach(func() {
				for i := 0; i < 5; i++ {
					tags = append(tags, generateAlias(16))
				}
				tag = tags[0]
			})
			It("'attach tag' should work", func() {
				err = integer.AttachTag(tag)
				Expect(err).ToNot(HaveOccurred())
			})
			It("'attach tags' should work", func() {
				err = integer.AttachTags(tags)
				Expect(err).ToNot(HaveOccurred())
			})
			Context("Attach tags before", func() {
				JustBeforeEach(func() {
					integer.AttachTags(tags)
				})
				AfterEach(func() {
					integer.DetachTags(tags)
				})
				It("'detach tag' should work", func() {
					err = integer.DetachTag(tag)
					Expect(err).ToNot(HaveOccurred())
				})
				It("'has tag' should work", func() {
					err = integer.HasTag(tag)
					Expect(err).ToNot(HaveOccurred())
				})
				It("'detach tags' should work", func() {
					err = integer.DetachTags(tags)
					Expect(err).ToNot(HaveOccurred())
				})
				It("'get tagged' should work", func() {
					aliasesObtained, err := integer.GetTagged(tag)
					Expect(err).ToNot(HaveOccurred())
					Expect(1).To(Equal(len(aliasesObtained)))
					Expect(alias).To(Equal(aliasesObtained[0]))
				})
				It("'get tags' should work", func() {
					tagsObtained, err := integer.GetTags()
					Expect(err).ToNot(HaveOccurred())
					Expect(len(tags)).To(Equal(len(tagsObtained)))
					Expect(tags).To(ConsistOf(tagsObtained))
				})
			})
			Context("Empty tag(s)", func() {
				BeforeEach(func() {
					tag = ""
					tags = []string{}
				})
				It("'attach tag' should not work", func() {
					err = integer.AttachTag(tag)
					Expect(err).To(HaveOccurred())
				})
				It("'attach tags' should not work", func() {
					err = integer.AttachTags(tags)
					Expect(err).To(HaveOccurred())
				})
				It("'has tag' should not work", func() {
					err = integer.HasTag(tag)
					Expect(err).To(HaveOccurred())
				})
				It("'detach tag' should not work", func() {
					err = integer.DetachTag(tag)
					Expect(err).To(HaveOccurred())
				})
				It("'detach tags' should not work", func() {
					err = integer.DetachTags(tags)
					Expect(err).To(HaveOccurred())
				})
				It("'get tagged' should not work", func() {
					aliasesObtained, err := integer.GetTagged(tag)
					Expect(err).To(HaveOccurred())
					Expect(0).To(Equal(len(aliasesObtained)))
				})
			})
		})

		// Expiry tests
		Context("Expiry", func() {
			var (
				expiry   time.Time
				duration time.Duration
			)
			JustBeforeEach(func() {
				now := time.Now()
				expiry = now.Add(duration)
			})
			Context("Distant future", func() {
				BeforeEach(func() {
					distantTime := time.Date(2040, 0, 0, 0, 0, 0, 0, time.UTC)
					duration = time.Until(distantTime)
				})
				It("should set expire at", func() {
					err = integer.ExpiresAt(expiry)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should set expire from now", func() {
					err = integer.ExpiresFromNow(duration)
					Expect(err).ToNot(HaveOccurred())
				})
			})
			Context("Short future", func() {
				BeforeEach(func() {
					duration, _ = time.ParseDuration("1h")
				})
				It("should set expire at", func() {
					err = integer.ExpiresAt(expiry)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should set expire from now", func() {
					err = integer.ExpiresFromNow(duration)
					Expect(err).ToNot(HaveOccurred())
				})
			})
			Context("Ultra short future", func() {
				BeforeEach(func() {
					duration, _ = time.ParseDuration("1µs")
				})
				It("should set expire at", func() {
					err = integer.ExpiresAt(expiry)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should set expire from now", func() {
					err = integer.ExpiresFromNow(duration)
					Expect(err).ToNot(HaveOccurred())
				})
			})
			Context("Short past", func() {
				BeforeEach(func() {
					duration, _ = time.ParseDuration("-1h")
				})
				It("should not set expire at", func() {
					err = integer.ExpiresAt(expiry)
					Expect(err).To(HaveOccurred())
				})
				It("should not set expire from now", func() {
					err = integer.ExpiresFromNow(duration)
					Expect(err).To(HaveOccurred())
				})
			})
			Context("Ultra short past", func() {
				BeforeEach(func() {
					duration, _ = time.ParseDuration("-5m30s")
				})
				It("should not set expire at", func() {
					err = integer.ExpiresAt(expiry)
					Expect(err).To(HaveOccurred())
				})
				It("should not set expire from now", func() {
					err = integer.ExpiresFromNow(duration)
					Expect(err).To(HaveOccurred())
				})
			})
		})

		// Location tests
		Context("Location", func() {
			var (
				port    int
				address string
			)
			BeforeEach(func() {
				port, err = strconv.Atoi(getenv("QDB_PORT", "2836"))
				address = getenv("QDB_HOST", "127.0.0.1")
			})
			It("should locate", func() {
				location, err := integer.GetLocation()
				Expect(err).ToNot(HaveOccurred())
				Expect(int16(port)).To(Equal(location.Port))
				Expect(address).To(Equal(location.Address))
			})
		})

		// Metadata tests
		Context("Location", func() {
			It("should locate", func() {
				metadata, err := integer.GetMetadata()
				Expect(err).ToNot(HaveOccurred())
				Expect(EntryInteger).To(Equal(metadata.Type))
			})
		})
	})

	// :: Blob tests ::
	Context("Blob", func() {
		var (
			blob       BlobEntry
			content    []byte
			newContent []byte
			badContent []byte
		)
		BeforeEach(func() {
			content = []byte("content")
			newContent = []byte("newContent")
			badContent = []byte("badContent")
		})
		JustBeforeEach(func() {
			blob = currentHandle.Blob(alias)
		})
		AfterEach(func() {
			blob.Remove()
		})
		Context("Empty content", func() {
			BeforeEach(func() {
				content = []byte{}
			})
			It("should put", func() {
				err := blob.Put(content, NeverExpires())
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := blob.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
			})
			It("should update", func() {
				err := blob.Update(content, NeverExpires())
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := blob.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
			})
			Context("Put before test", func() {
				JustBeforeEach(func() {
					blob.Put(content, NeverExpires())
				})
				It("should get", func() {
					contentObtained, err := blob.Get()
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
				})
				It("should 'get and update'", func() {
					contentObtained, err := blob.GetAndUpdate(newContent, PreserveExpiration())
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
					contentObtained, err = blob.Get()
					Expect(err).ToNot(HaveOccurred())
					Expect(newContent).To(Equal(contentObtained))
				})
				It("should 'get and remove'", func() {
					contentObtained, err := blob.GetAndRemove()
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
					contentObtained, err = blob.Get()
					Expect(err).To(HaveOccurred())
					Expect([]byte{}).To(Equal(contentObtained))
				})
				It("should 'remove if'", func() {
					comparand := content
					err := blob.RemoveIf(comparand)
					Expect(err).ToNot(HaveOccurred())
					contentObtained, err := blob.Get()
					Expect(err).To(HaveOccurred())
					Expect([]byte{}).To(Equal(contentObtained))
				})
				It("should not 'remove if' with bad content", func() {
					comparand := badContent
					err := blob.RemoveIf(comparand)
					Expect(err).To(HaveOccurred())
				})
				It("should 'compare and swap' with good content", func() {
					comparand := content
					contentObtained, err := blob.CompareAndSwap(newContent, comparand, PreserveExpiration())
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
					contentObtained, err = blob.Get()
					Expect(err).ToNot(HaveOccurred())
					Expect(newContent).To(Equal(contentObtained))
				})
				It("should not 'compare and swap' with bad content", func() {
					comparand := badContent
					contentObtained, err := blob.CompareAndSwap(newContent, comparand, PreserveExpiration())
					Expect(err).To(HaveOccurred())
					Expect([]byte{}).To(Equal(contentObtained))
				})
			})
		})
		Context("Normal content", func() {
			BeforeEach(func() {
				newContent = []byte{}
			})
			It("should put", func() {
				err := blob.Put(content, NeverExpires())
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := blob.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
			})
			It("should update", func() {
				err := blob.Update(content, NeverExpires())
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := blob.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
			})
			Context("Put before test", func() {
				JustBeforeEach(func() {
					blob.Put(content, NeverExpires())
				})
				It("should get", func() {
					contentObtained, err := blob.Get()
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
				})
				It("should 'get and update'", func() {
					contentObtained, err := blob.GetAndUpdate(newContent, PreserveExpiration())
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
					contentObtained, err = blob.Get()
					Expect(err).ToNot(HaveOccurred())
					Expect(newContent).To(Equal(contentObtained))
				})
				It("should 'get and remove'", func() {
					contentObtained, err := blob.GetAndRemove()
					Expect(err).ToNot(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
					contentObtained, err = blob.Get()
					Expect(err).To(HaveOccurred())
					Expect([]byte{}).To(Equal(contentObtained))
				})
				It("should 'remove if'", func() {
					comparand := content
					err := blob.RemoveIf(comparand)
					Expect(err).ToNot(HaveOccurred())
					contentObtained, err := blob.Get()
					Expect(err).To(HaveOccurred())
					Expect([]byte{}).To(Equal(contentObtained))
				})
				It("should not 'remove if' with bad content", func() {
					comparand := []byte("badContent")
					err := blob.RemoveIf(comparand)
					Expect(err).To(HaveOccurred())
				})
				It("should 'compare and swap' with good content", func() {
					comparand := content
					contentObtained, err := blob.CompareAndSwap(newContent, comparand, PreserveExpiration())
					Expect(err).ToNot(HaveOccurred())
					Expect([]byte{}).To(Equal(contentObtained))
					contentObtained, err = blob.Get()
					Expect(err).ToNot(HaveOccurred())
					Expect(newContent).To(Equal(contentObtained))
				})
				It("should not 'compare and swap' with bad content", func() {
					comparand := []byte("badContent")
					contentObtained, err := blob.CompareAndSwap(newContent, comparand, PreserveExpiration())
					Expect(err).To(HaveOccurred())
					Expect(content).To(Equal(contentObtained))
				})
			})
		})
	})

	// :: Integer tests ::
	Context("Integer", func() {
		var (
			integer    IntegerEntry
			content    int64
			newContent int64
		)
		BeforeEach(func() {
			content = 13
			newContent = 87
			integer = currentHandle.Integer(alias)
		})
		AfterEach(func() {
			integer.Remove()
		})
		It("should put", func() {
			err := integer.Put(content, NeverExpires())
			Expect(err).ToNot(HaveOccurred())
		})
		It("should not put again", func() {
			err := integer.Put(content, NeverExpires())
			Expect(err).ToNot(HaveOccurred())
			err = integer.Put(content, NeverExpires())
			Expect(err).To(HaveOccurred())
		})
		Context("Put before tests", func() {
			JustBeforeEach(func() {
				integer.Put(content, NeverExpires())
			})
			It("should update", func() {
				err := integer.Update(newContent, NeverExpires())
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := integer.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(newContent).To(Equal(contentObtained))
			})
			It("should get", func() {
				contentObtained, err := integer.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
			})
			It("should add", func() {
				toAdd := int64(5)
				expected := toAdd + content
				contentObtained, err := integer.Add(toAdd)
				Expect(err).ToNot(HaveOccurred())
				Expect(expected).To(Equal(contentObtained))
			})
			It("should remove", func() {
				err := integer.Remove()
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := integer.Get()
				Expect(err).To(HaveOccurred())
				Expect(int64(0)).To(Equal(contentObtained))
			})
		})
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
			timeseries = currentHandle.Timeseries(alias, columnsInfo)
		})
		AfterEach(func() {
			timeseries.Remove()
		})
		Context("Empty columns info", func() {
			BeforeEach(func() {
				columnsInfo = []TsColumnInfo{}
			})
			It("should not create", func() {
				err = timeseries.Create()
				Expect(err).To(HaveOccurred())
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
				err = timeseries.Create()
				Expect(err).ToNot(HaveOccurred())
			})
			Context("Insert", func() {
				It("should work to insert blob points", func() {
					err = timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should work to insert double points", func() {
					err = timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
					Expect(err).ToNot(HaveOccurred())
				})
				Context("Empty Points Array", func() {
					BeforeEach(func() {
						doublePoints = []TsDoublePoint{}
						blobPoints = []TsBlobPoint{}
					})
					It("should not work to insert double points", func() {
						err = timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
						Expect(err).To(HaveOccurred())
					})
					It("should not work to insert blob points", func() {
						err = timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
						Expect(err).To(HaveOccurred())
					})
				})
			})
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
				// doubleAggs TsDoubleAggregations
				// blobAggs   TsBlobAggregations
				)
				JustBeforeEach(func() {
					timeseries.InsertDouble(timeseries.columns[1].Name, doublePoints)
					timeseries.InsertBlob(timeseries.columns[0].Name, blobPoints)
				})
			})

		})
	})
})
