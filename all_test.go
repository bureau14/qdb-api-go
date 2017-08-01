package qdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	qdbPath string
	qdbPort = 30083
)

func TestMain(m *testing.M) {
	qdbPath = os.Getenv("QDB_SERVER_PATH")
	qdbServerSecurityAppPath := os.Getenv("QDB_SERVER_SECURITY_PATH")
	qdbUserSecurityAppPath := os.Getenv("QDB_USER_SECURITY_PATH")
	if qdbPath == "" {
		fmt.Printf("No path found for qdb server\n")
		os.Exit(-1)
	}
	fmt.Printf("Copying qdb server: %s\n", qdbPath)
	qdbPath = createLocalQdbExe(qdbPath)
	qdbPort = startQdbServer(qdbPath, qdbServerSecurityAppPath, qdbUserSecurityAppPath)

	m.Run()

	stopQdbServer(qdbPath)
	removeLocalQdb(qdbPath, qdbServerSecurityAppPath, qdbUserSecurityAppPath)
}

func TestAll(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var _ = Describe("Tests", func() {
	var (
		handle    HandleType
		alias     string
		err       error
		isSecured bool
	)
	BeforeSuite(func() {
		handle = CreateExampleHandle()
		isSecured = !(os.Getenv("QDB_SERVER_SECURITY_PATH") != "" && os.Getenv("QDB_USER_SECURITY_PATH") != "")

		// stupid thing to boast about having 100% test coverage
		fmt.Errorf("error: %s", ErrorType(2))
	})

	AfterSuite(func() {
		handle.Close()
	})

	BeforeEach(func() {
		alias = generateAlias(16)
	})

	Context("Handle", func() {
		var (
			testHandle HandleType
			clusterURI string
		)
		BeforeEach(func() {
			clusterURI = fmt.Sprintf("qdb://127.0.0.1:%d", qdbPort)
		})
		It("should not connect without creating handle", func() {
			err := testHandle.Connect(clusterURI)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to open with random protocol", func() {
			err := testHandle.Open(2)
			Expect(err).To(HaveOccurred())
		})
		It("should be able to open with TCP protocol", func() {
			err := testHandle.Open(ProtocolTCP)
			Expect(err).ToNot(HaveOccurred())
			testHandle.Close()
		})
		It("should setup an handle", func() {
			if !isSecured {
				Skip("Skipped since it only creates a handle for non secured database")
			}
			handle, err := SetupHandle(clusterURI)
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should not be able to setup an handle", func() {
			if isSecured {
				Skip("Skipped since it only fails if security mode is activated")
			}
			_, err := SetupHandle(clusterURI)
			Expect(err).To(HaveOccurred())
		})
		It("must setup an handle", func() {
			if !isSecured {
				Skip("Skipped since it only creates a handle for non secured database")
			}
			handle := MustSetupHandle(clusterURI)
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should setup a secured handle", func() {
			if isSecured {
				Skip("Skipped since it only creates a handle for secured database")
			}
			handle, err := SetupSecuredHandle(clusterURI, "cluster_public.key", "user_private.key")
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should not be able setup a secured handle", func() {
			if !isSecured {
				Skip("Skipped since it only fails if security mode is deactivated")
			}
			_, err := SetupSecuredHandle(clusterURI, "cluster_public.key", "user_private.key")
			Expect(err).To(HaveOccurred())
		})
		It("must setup a secured handle", func() {
			if isSecured {
				Skip("Skipped since it only creates a handle for secured database")
			}
			handle := MustSetupSecuredHandle(clusterURI, "cluster_public.key", "user_private.key")
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		Context("With Handle", func() {
			BeforeEach(func() {
				testHandle, err = NewHandle()
				if os.Getenv("QDB_SERVER_SECURITY_PATH") != "" && os.Getenv("QDB_USER_SECURITY_PATH") != "" {
					err := testHandle.AddClusterPublicKey("cluster_public.key")
					Expect(err).ToNot(HaveOccurred())
					err = testHandle.AddUserCredentials("user_private.key")
					Expect(err).ToNot(HaveOccurred())
				}
			})
			It("should not add cluster public key with invalid filename", func() {
				err := testHandle.AddClusterPublicKey("asd")
				Expect(err).To(HaveOccurred())
			})
			It("should add cluster public key with valid file", func() {
				ioutil.WriteFile("test.key", []byte("PPm6ZeBCVlDTR9xtYasXd31s8rXnQpb+CNTMohOlQqBw="), 0777)
				err := testHandle.AddClusterPublicKey("test.key")
				Expect(err).ToNot(HaveOccurred())
				os.Remove("test.key")
			})
			It("should not add credentials with invalid filename", func() {
				err := testHandle.AddUserCredentials("asd")
				Expect(err).To(HaveOccurred())
			})
			It("should not add credentials with invalid file", func() {
				err := testHandle.AddUserCredentials("error.go")
				Expect(err).To(HaveOccurred())
			})
			It("should add credentials with valid file", func() {
				ioutil.WriteFile("test.key", []byte("{\"username\": \"vianney\",\"secret_key\": \"SeVUamemy6GWb8npfh9lum1zhdAu76W+l0PAW03G5yl4=\"}"), 0777)
				err := testHandle.AddUserCredentials("test.key")
				Expect(err).ToNot(HaveOccurred())
				os.Remove("test.key")
			})
			It("should not connect without address", func() {
				err := testHandle.Connect("")
				Expect(err).To(HaveOccurred())
			})
			It("should connect", func() {
				err := testHandle.Connect(clusterURI)
				Expect(err).ToNot(HaveOccurred())
				testHandle.Close()
			})
			Context("With Connection established", func() {
				BeforeEach(func() {
					testHandle.Connect(clusterURI)
				})
				AfterEach(func() {
					testHandle.Close()
				})
				It("should not return an empty version", func() {
					apiVersion := handle.APIVersion()
					Expect("").ToNot(Equal(apiVersion))
				})
				It("should not return an empty build", func() {
					apiVersion := handle.APIBuild()
					Expect("").ToNot(Equal(apiVersion))
				})
				It("should set timeout to 1s", func() {
					err := handle.SetTimeout(1000)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able set timeout to 0ms", func() {
					err := handle.SetTimeout(0)
					Expect(err).To(HaveOccurred())
				})
				It("should be able to 'set max cardinality' with default value", func() {
					err := handle.SetMaxCardinality(10007)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able to 'set max cardinality' with value under 100", func() {
					err := handle.SetMaxCardinality(99)
					Expect(err).To(HaveOccurred())
				})
				It("should be able to 'set compression' to fast", func() {
					err := handle.SetCompression(CompFast)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able to call 'set compression' with random value", func() {
					err := handle.SetCompression(5)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})

	// :: Entry tests ::
	Context("Entry", func() {
		var (
			integer IntegerEntry
		)
		JustBeforeEach(func() {
			integer = handle.Integer(alias)
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
			It("'get tags' should work with no tags added", func() {
				tags, err := integer.GetTags()
				Expect(err).ToNot(HaveOccurred())
				Expect([]string{}).To(Equal(tags))
			})
			It("'get tags' should not work with removed alias", func() {
				integer.Remove()
				tags, err := integer.GetTags()
				Expect(err).To(HaveOccurred())
				Expect([]string(nil)).To(Equal(tags))
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
		// TODO(vianney): check expiry values (with getmetadata)
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
		// May not work with more complex port or addresses
		Context("Location", func() {
			var (
				address string
			)
			BeforeEach(func() {
				address = "127.0.0.1"
			})
			It("should locate", func() {
				location, err := integer.GetLocation()
				Expect(err).ToNot(HaveOccurred())
				Expect(int16(qdbPort)).To(Equal(location.Port))
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
			blob = handle.Blob(alias)
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
			integer = handle.Integer(alias)
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
				sum := toAdd + content
				result, err := integer.Add(toAdd)
				Expect(err).ToNot(HaveOccurred())
				Expect(sum).To(Equal(result))
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
				err = timeseries.Create()
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

func createLocalQdbExe(qdbPath string) string {
	localQdbName := string("test_qdbd")
	exec.Command("cp", qdbPath, localQdbName).Output()
	return localQdbName
}

func removeLocalQdb(qdbPath, qdbServerSecurityAppPath, qdbUserSecurityAppPath string) {
	os.Remove(qdbPath)
	os.RemoveAll("db/")
	if qdbServerSecurityAppPath != "" && qdbUserSecurityAppPath != "" {
		os.Remove("cluster_private.key")
		os.Remove("cluster_public.key")
		os.Remove("user_private.key")
		os.Remove("users.cfg")
	}
}

func stopQdbServer(qdbPath string) {
	exec.Command("killall", fmt.Sprint("./", qdbPath)).Output()
}

func startQdbServer(qdbPath, qdbServerSecurityAppPath, qdbUserSecurityAppPath string) int {
	// random := rand.Intn(1000)
	port := qdbPort
	exe := "./"
	exe += qdbPath
	fmt.Printf("Opening %s on port %d\n", qdbPath, port)
	address := fmt.Sprint("127.0.0.1:", port)

	var runQdbServer *exec.Cmd
	if qdbServerSecurityAppPath != "" && qdbUserSecurityAppPath != "" {
		exec.Command(qdbServerSecurityAppPath, "-p", "cluster_public.key", "-s", "cluster_private.key").Output()
		exec.Command(qdbUserSecurityAppPath, "-u", "test", "-p", "users.cfg", "-s", "user_private.key").Output()
		runQdbServer = exec.Command(exe, "--cluster-private-file", "cluster_private.key", "--user-list", "users.cfg", "-a", address)
	} else {
		runQdbServer = exec.Command(exe, "--security=false", "-a", address)
	}
	var outbuf, errbuf bytes.Buffer
	runQdbServer.Stdout = &outbuf
	runQdbServer.Stderr = &errbuf
	runQdbServer.Start()

	time.Sleep(5 * time.Second)
	return port
}

func CreateExampleHandle() HandleType {
	var handle HandleType
	clusterURI := fmt.Sprintf("qdb://127.0.0.1:%d", qdbPort)
	if os.Getenv("QDB_SERVER_SECURITY_PATH") != "" && os.Getenv("QDB_USER_SECURITY_PATH") != "" {
		handle = MustSetupSecuredHandle(clusterURI, "cluster_public.key", "user_private.key")
	} else {
		handle = MustSetupHandle(clusterURI)
	}
	return handle
}
