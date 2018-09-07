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
		Context("Status", func() {
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
				Expect(status.Network.Partitions.MaxSessions).To(Equal(64))
			})
		})

		Context("Config", func() {
			It("should not retrieve status with empty uri", func() {
				_, err = handle.Node("").Config()
				Expect(err).To(HaveOccurred())
			})
			It("should not retrieve status with invalid uri", func() {
				_, err = handle.Node("qdb://127.0.0.1:36321").Config()
				Expect(err).To(HaveOccurred())
			})
			It("should retrieve status with valid uri", func() {
				config, err := handle.Node("qdb://127.0.0.1:30083").Config()
				Expect(err).ToNot(HaveOccurred())
				Expect(config.Local.Depot.Root).To(Equal("db"))
			})
		})

		Context("Topology", func() {
			It("should not retrieve status with empty uri", func() {
				_, err = handle.Node("").Topology()
				Expect(err).To(HaveOccurred())
			})
			It("should not retrieve status with invalid uri", func() {
				_, err = handle.Node("qdb://127.0.0.1:36321").Topology()
				Expect(err).To(HaveOccurred())
			})
			It("should retrieve status with valid uri", func() {
				topology, err := handle.Node("qdb://127.0.0.1:30083").Topology()
				Expect(err).ToNot(HaveOccurred())
				Expect(topology.Successor.Endpoint).To(Equal(topology.Predecessor.Endpoint))
			})
		})
	})
})
