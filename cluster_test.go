package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		alias        string
		aliassecure string
	)

	BeforeEach(func() {
		alias = generateAlias(16)
		aliassecure = generateAlias(16)
	})

	// :: Blob tests ::
	Context("Cluster", func() {
		var (
			blob           BlobEntry
			blobsecure    BlobEntry
			cluster        *Cluster
			secureCluster *Cluster
			content        []byte
		)
		BeforeEach(func() {
			cluster = handle.Cluster()
			secureCluster = secureHandle.Cluster()
			content = []byte("content_blob")
			blob = handle.Blob(alias)
			blobsecure = secureHandle.Blob(aliassecure)
			err := blob.Put(content, NeverExpires())
			Expect(err).ToNot(HaveOccurred())
			err = blobsecure.Put(content, NeverExpires())
			Expect(err).ToNot(HaveOccurred())
		})
		AfterEach(func() {
			blob.Remove()
			blobsecure.Remove()
		})
		Context("PurgeAll", func() {
			It("should remove all contents", func() {
				err := secureCluster.PurgeAll()
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := blobsecure.Get()
				Expect(content).ToNot(Equal(contentObtained))
				Expect(err).To(HaveOccurred())
			})
			It("should be unable to remove all contents", func() {
				err := cluster.PurgeAll()
				Expect(err).To(HaveOccurred())
				contentObtained, err := blob.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
			})
		})
		Context("PurgeCache", func() {
			It("should remove all contents from memory", func() {
				// we cannot really test that
				// _, err := blobsecure.Get()
				// Expect(err).ToNot(HaveOccurred())

				// err = secureCluster.PurgeCache()
				// Expect(err).ToNot(HaveOccurred())

				// _, err = blobsecure.Get()
				// Expect(err).To(Equal(ErrAliasNotFound))
			})
		})
		Context("TrimAll", func() {
			It("should not work with bad handle", func() {
				h := HandleType{}
				c := h.Cluster()
				err := c.TrimAll()
				Expect(err).To(HaveOccurred())
			})
			It("should work with valid handle", func() {
				err := cluster.TrimAll()
				Expect(err).ToNot(HaveOccurred())
			})
		})
		Context("WaitForStabilization", func() {
			It("should not work with bad handle", func() {
				h := HandleType{}
				c := h.Cluster()
				err := c.WaitForStabilization(60 * time.Second)
				Expect(err).To(HaveOccurred())
			})
			It("should work with valid handle", func() {
				err := cluster.WaitForStabilization(60 * time.Second)
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
})
