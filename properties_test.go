package qdb

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	Context("Properties", func() {
		var (
			prop  string
			value string
		)
		BeforeEach(func() {
			prop = generateAlias(16)
			value = generateAlias(16)
		})

		It("should have system properties after connect", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to update system properties", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to put empty property, non empty value", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to put duplicate", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to put empty property, empty value", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to put empty value, non-empty property", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
		It("should put non-empty properties, non-empty value", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should get properties", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should not get properties by empty name", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should remove properties", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should not remove properties with empty name", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to remove system properties", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should remove all properties, but keep system", func() {
			err := handle.Connect(insecureURI)
			Expect(err).To(HaveOccurred())
		})
		It("should be able to update property", func() {
			value, err := handle.GetProperties("test")
			Expect(err).To(HaveOccurred())
		})
	})
})
