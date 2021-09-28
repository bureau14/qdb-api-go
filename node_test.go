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
		handle, err = SetupHandle(insecureURI, 120*time.Second)
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
				status, err := handle.Node(insecureURI).Status()
				Expect(err).ToNot(HaveOccurred())
				Expect(status.Network.Partitions.MaxSessions).To(Equal(512))
			})
		})

		Context("Config", func() {
			It("should not retrieve config with empty uri", func() {
				_, err = handle.Node("").Config()
				Expect(err).To(HaveOccurred())
			})
			It("should not retrieve config with invalid uri", func() {
				_, err = handle.Node("qdb://127.0.0.1:36321").Config()
				Expect(err).To(HaveOccurred())
			})
			It("should retrieve config with valid uri", func() {
				config, err := handle.Node(insecureURI).Config()
				Expect(err).ToNot(HaveOccurred())
				Expect(config.Local.Depot.RocksDB.Root).To(Equal("insecure/db"))
			})
		})

		Context("Topology", func() {
			It("should not retrieve topology with empty uri", func() {
				_, err = handle.Node("").Topology()
				Expect(err).To(HaveOccurred())
			})
			It("should not retrieve topology with invalid uri", func() {
				_, err = handle.Node("qdb://127.0.0.1:36321").Topology()
				Expect(err).To(HaveOccurred())
			})
			It("should retrieve topology with valid uri", func() {
				topology, err := handle.Node(insecureURI).Topology()
				Expect(err).ToNot(HaveOccurred())
				Expect(topology.Successor.Endpoint).To(Equal(topology.Predecessor.Endpoint))
			})
		})
	})
})
