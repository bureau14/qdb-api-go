package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		handle HandleType
		alias  string
		err    error
	)

	BeforeEach(func() {
		handle, err = SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
		Expect(err).ToNot(HaveOccurred())
		alias = generateAlias(16)
	})

	AfterEach(func() {
		handle.Close()
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
})
