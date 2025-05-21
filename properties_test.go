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
			apiName, err := handle.GetProperties("api")
			apiVersion, err := handle.GetProperties("api_version")
			Expect(err).ToNot(HaveOccurred())
			Expect(apiName).To(Equal("go"))
			Expect(apiVersion).To(Equal(GitHash))
		})
		It("should not be able to update system properties", func() {
			errNameUpdate := handle.UpdateProperties("api", "fake_api")
			errVersionUpdate := handle.UpdateProperties("api_version", "fake_api_version")
			Expect(errNameUpdate).To(HaveOccurred())
			Expect(errVersionUpdate).To(HaveOccurred())
		})
		It("should not be able to put empty property, non empty value", func() {
			err := handle.PutProperties("", value)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to put duplicate", func() {
			err := handle.PutProperties(prop, value)
			err = handle.PutProperties(prop, value+"_1")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to put empty value, non-empty property", func() {
			err := handle.PutProperties(prop, "")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidArgument))
		})
		It("should get properties", func() {
			putErr := handle.PutProperties(prop, value)
			Expect(putErr).ToNot(HaveOccurred())
			returnedValue, err := handle.GetProperties(prop)
			Expect(err).ToNot(HaveOccurred())
			Expect(returnedValue).To(Equal(value))
		})
		It("should not get properties by empty name", func() {
			_, err := handle.GetProperties("")
			Expect(err).To(HaveOccurred())
		})
		It("should remove properties", func() {
			propName := generateAlias(16)
			putErr := handle.PutProperties(propName, value)
			Expect(putErr).ToNot(HaveOccurred())
			err := handle.RemoveProperties(propName)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should not remove properties with empty name", func() {
			err := handle.RemoveProperties("")
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to remove system properties", func() {
			err := handle.RemoveProperties("api")
			err = handle.RemoveProperties("api_version")
			Expect(err).To(HaveOccurred())
		})
		It("should remove all properties, but keep system", func() {
			err := handle.RemoveAllProperties()
			Expect(err).ToNot(HaveOccurred())
			apiName, _ := handle.GetProperties("api")
			apiVersion, _ := handle.GetProperties("api_version")
			Expect(apiName).To(Equal("go"))
			Expect(apiVersion).To(Equal(GitHash))
		})
		It("should be able to update property", func() {
			err := handle.UpdateProperties(prop, value+"_new")
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be able to update non-existent property", func() {
			err := handle.UpdateProperties(generateAlias(16), "test")
			Expect(err).ToNot(HaveOccurred())
		})
		It("should not be able to update empty property", func() {
			err := handle.UpdateProperties("", "test")
			Expect(err).To(HaveOccurred())
		})
	})
})
