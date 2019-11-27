package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		alias string
		err   error
	)

	BeforeEach(func() {
		alias = generateAlias(16)
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
				tags = []string{"asd", "dsa", "ede", "esd", "fds"}
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
					err = integer.AttachTags(tags)
					Expect(err).ToNot(HaveOccurred())
				})
				AfterEach(func() {
					err = integer.DetachTags(tags)
					Expect(err).ToNot(HaveOccurred())
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
				It("'get tagged' should return empty output when provided wrong tag", func() {
					aliasesObtained, err := integer.GetTagged("asd")
					Expect(err).ToNot(HaveOccurred())
					Expect(0).To(Equal(len(aliasesObtained)))
					Expect([]string{}).To(Equal(aliasesObtained))
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
					duration = distantTime.Sub(time.Now())
				})
				It("should set expire at", func() {
					err = integer.ExpiresAt(expiry)
					Expect(err).ToNot(HaveOccurred())

					meta, err := integer.GetMetadata()
					Expect(err).ToNot(HaveOccurred())
					Expect(toQdbTime(meta.ExpiryTime)).To(Equal(toQdbTime(expiry)))
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

					meta, err := integer.GetMetadata()
					Expect(err).ToNot(HaveOccurred())
					Expect(toQdbTime(meta.ExpiryTime)).To(Equal(toQdbTime(expiry)))
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
			Context("Basic functions", func() {
				BeforeEach(func() {
					duration, _ = time.ParseDuration("1h")
				})
				It("should retrieve the right result", func() {
					meta, err := integer.GetMetadata()
					Expect(err).ToNot(HaveOccurred())
					Expect(meta.ExpiryTime).To(Equal(NeverExpires()))
				})
				It("should retrieve the right result", func() {
					integer.Update(12, expiry)
					integer.Update(14, PreserveExpiration())

					meta, err := integer.GetMetadata()
					Expect(err).ToNot(HaveOccurred())
					Expect(toQdbTime(meta.ExpiryTime)).To(Equal(toQdbTime(expiry)))
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
				Expect(int16(2836)).To(Equal(location.Port))
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
})
