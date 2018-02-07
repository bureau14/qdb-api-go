package qdb

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		aliases []string
		blob1   BlobEntry
		blob2   BlobEntry
		integer IntegerEntry
	)

	BeforeEach(func() {
		var err error
		aliases = append(aliases, "blob first")
		aliases = append(aliases, "blob second")
		aliases = append(aliases, "integer first")

		blob1 = handle.Blob(aliases[0])
		err = blob1.Put([]byte("asd"), NeverExpires())
		Expect(err).ToNot(HaveOccurred())
		err = blob1.AttachTag("all")
		Expect(err).ToNot(HaveOccurred())
		err = blob1.AttachTag("first")
		Expect(err).ToNot(HaveOccurred())

		blob2 = handle.Blob(aliases[1])
		err = blob2.Put([]byte("asd"), NeverExpires())
		Expect(err).ToNot(HaveOccurred())
		err = blob2.AttachTag("all")
		Expect(err).ToNot(HaveOccurred())
		err = blob2.AttachTag("second")
		Expect(err).ToNot(HaveOccurred())

		integer = handle.Integer(aliases[2])
		err = integer.Put(32, NeverExpires())
		Expect(err).ToNot(HaveOccurred())
		err = integer.AttachTag("all")
		Expect(err).ToNot(HaveOccurred())
		err = integer.AttachTag("third")
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		blob1.Remove()
		blob2.Remove()
		integer.Remove()
		aliases = []string{}
	})
	// :: Entry tests ::
	Context("Query", func() {
		It("should get all aliases", func() {
			obtainedAliases, err := handle.Query().Tag("all").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(3))
			Expect(obtainedAliases).To(ConsistOf(aliases))
		})
		It("should get both first, and third aliases", func() {
			obtainedAliases, err := handle.Query().Tag("all").NotTag("second").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(2))
			Expect(obtainedAliases).To(ConsistOf([]string{blob1.Alias(), integer.Alias()}))
		})
		It("should not get the not \"second\" tags", func() {
			obtainedAliases, err := handle.Query().NotTag("second").Execute()
			Expect(err).To(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(0))
			Expect(obtainedAliases).To(ConsistOf([]string(nil)))
		})
		It("should get the third alias by getting both \"all\" and \"third\"", func() {
			obtainedAliases, err := handle.Query().Tag("all").Tag("third").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(1))
			Expect(obtainedAliases).To(ConsistOf([]string{integer.Alias()}))
		})
		It("should get no results because tags are not compatible", func() {
			obtainedAliases, err := handle.Query().Tag("first").Tag("third").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(0))
			Expect(obtainedAliases).To(ConsistOf([]string{}))
		})
		It("should get both blob elements", func() {
			obtainedAliases, err := handle.Query().Tag("all").Type("blob").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(2))
			Expect(obtainedAliases).To(ConsistOf([]string{blob1.Alias(), blob2.Alias()}))
		})
		It("should not be able to simply call with a type", func() {
			obtainedAliases, err := handle.Query().Type("blob").Execute()
			Expect(err).To(HaveOccurred())
			Expect(obtainedAliases).To(Equal([]string(nil)))
		})
		It("should get integer element only", func() {
			obtainedAliases, err := handle.Query().Tag("all").Type("int").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(1))
			Expect(obtainedAliases).To(ConsistOf([]string{integer.Alias()}))
		})
		It("should not be able to retrieve anything", func() {
			obtainedAliases, err := handle.Query().Tag("dsas").Execute()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(0))
			Expect(obtainedAliases).To(Equal([]string{}))
		})
		It("should not be able to use a bad type", func() {
			obtainedAliases, err := handle.Query().Tag("all").Type("dsas").Execute()
			Expect(err).To(HaveOccurred())
			Expect(obtainedAliases).To(Equal([]string(nil)))
		})
		It("should be able to execute a string quert with valid input", func() {
			obtainedAliases, err := handle.Query().ExecuteString("find(tag=\"all\")")
			Expect(err).ToNot(HaveOccurred())
			Expect(len(obtainedAliases)).To(Equal(3))
			Expect(obtainedAliases).To(ConsistOf(aliases))
		})
	})
})
