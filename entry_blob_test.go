package qdb

import (
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
})
