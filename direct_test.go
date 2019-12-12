package qdb

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	Context("DirectHandle", func() {
		var directHandle DirectHandleType

		BeforeEach(func() {
			cluster := handle.Cluster()
			endPoints, err := cluster.Endpoints()
			endPoint := endPoints[0]

			Expect(err).ToNot(HaveOccurred())

			directHandle, err = handle.DirectConnect(endPoint.URI())

			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			directHandle.Close()
		})

		Context("PrefixGet", func() {
			It("should get prefixes", func() {
				entries, err := directHandle.PrefixGet("$qdb", 1000)

				Expect(err).ToNot(HaveOccurred())
				Expect(len(entries) > 0).To(BeTrue())
			})
		})

		Context("DirectBlobEntry", func() {
			var entry DirectBlobEntry

			BeforeEach(func() {
				alias := generateAlias(16)
				entry = directHandle.Blob(alias)
			})

			AfterEach(func() {
				entry.Remove()
			})

			It("should put a new entry", func() {
				Skip("C API not yet implemented")
				content := []byte("content")
				err := entry.Put(content, NeverExpires())
				Expect(err).ToNot(HaveOccurred())
			})

			It("should get an existing entry", func() {
				Skip("Waiting for implementation of put")
			})

			It("should update an existing entry", func() {
				Skip("Waiting for implementation of put")
			})
		})

		Context("DirectIntegerEntry", func() {
			var entry DirectIntegerEntry

			BeforeEach(func() {
				alias := generateAlias(16)
				entry = directHandle.Integer(alias)
			})

			AfterEach(func() {
				entry.Remove()
			})

			It("should put a new entry", func() {
				Skip("C API not yet implemented")
			})

			It("should get an existing entry", func() {
				Skip("Waiting for implementation of put")
			})

			It("should update an existing entry", func() {
				Skip("Waiting for implementation of put")
			})

			It("should add to an existing entry", func() {
				Skip("Waiting for implementation of put")
			})
		})
	})
})
