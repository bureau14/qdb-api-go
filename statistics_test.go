package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	Context("Statistics", func() {
		It("should work", func() {
			time.Sleep(5 * time.Second)
			results, err := handle.Statistics()
			Expect(err).ToNot(HaveOccurred())
			for _, result := range results {
				Expect(result.EngineVersion[:2]).To(Equal("3."))
			}
		})
	})
})
