package qdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	var (
		handle HandleType
		err    error
	)

	BeforeEach(func() {
		handle, err = SetupHandle("qdb://127.0.0.1:30083", 120*time.Second)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		handle.Close()
	})

	Context("Node", func() {
		It("should not retrieve status with empty uri", func() {
			_, err = handle.Node("").Status()
			Expect(err).To(HaveOccurred())
		})
		It("should not retrieve status with invalid uri", func() {
			_, err = handle.Node("qdb://127.0.0.1:36321").Status()
			Expect(err).To(HaveOccurred())
		})
		It("should retrieve status with valid uri", func() {
			status, err := handle.Node("qdb://127.0.0.1:30083").Status()
			Expect(err).ToNot(HaveOccurred())
			Expect(status.Network.Partitions.MaxSessions).To(Equal(20000))
		})
	})
})
