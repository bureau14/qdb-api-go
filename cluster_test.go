package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		alias        string
		aliasSecured string
	)

	BeforeEach(func() {
		alias = generateAlias(16)
		aliasSecured = generateAlias(16)
	})

	// :: Blob tests ::
	Context("Cluster", func() {
		var (
			blob           BlobEntry
			blobSecured    BlobEntry
			cluster        *Cluster
			securedCluster *Cluster
			content        []byte
		)
		BeforeEach(func() {
			cluster = handle.Cluster()
			securedCluster = securedHandle.Cluster()
			content = []byte("content_blob")
			blob = handle.Blob(alias)
			blobSecured = securedHandle.Blob(aliasSecured)
			err := blob.Put(content, NeverExpires())
			Expect(err).ToNot(HaveOccurred())
			err = blobSecured.Put(content, NeverExpires())
			Expect(err).ToNot(HaveOccurred())
		})
		AfterEach(func() {
			blob.Remove()
			blobSecured.Remove()
		})
		Context("PurgeAll", func() {
			It("should remove all contents", func() {
				err := securedCluster.PurgeAll()
				Expect(err).ToNot(HaveOccurred())
				contentObtained, err := blobSecured.Get()
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
				time.Sleep(5 * time.Second)
				stats, err := handle.Statistics()
				Expect(err).ToNot(HaveOccurred())
				var nodeStat Statistics
				for _, stat := range stats {
					nodeStat = stat
					break
				}
				Expect(int64(0)).ToNot(BeNumerically("==", nodeStat.Persistence.EntriesCount))

				err = cluster.PurgeCache()
				Expect(err).ToNot(HaveOccurred())

				stats, err = handle.Statistics()
				Expect(err).ToNot(HaveOccurred())
				for _, stat := range stats {
					nodeStat = stat
					break
				}
				// FIXME(vianney): should work but need to be run separately I believe
				// Expect(int64(0)).To(BeNumerically("==", nodeStat.Memory.ResidentCount))
				Expect(int64(0)).ToNot(BeNumerically("==", nodeStat.Persistence.EntriesCount))

				contentObtained, err := blob.Get()
				Expect(err).ToNot(HaveOccurred())
				Expect(content).To(Equal(contentObtained))
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
