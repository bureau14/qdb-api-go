package qdb

import (
	"io/ioutil"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tests", func() {
	Context("Handle", func() {
		var (
			testHandle HandleType
		)
		It("should not connect without creating handle", func() {
			err := testHandle.Connect(clusterURI)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able to open with random protocol", func() {
			err := testHandle.Open(2)
			Expect(err).To(HaveOccurred())
		})
		It("should be able to open with TCP protocol", func() {
			err := testHandle.Open(ProtocolTCP)
			Expect(err).ToNot(HaveOccurred())
			testHandle.Close()
		})
		It("should setup an handle", func() {
			handle, err := SetupHandle(clusterURI, time.Duration(120)*time.Second)
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should not be able to setup an handle", func() {
			_, err := SetupHandle(securedURI, time.Duration(120)*time.Second)
			Expect(err).To(HaveOccurred())
		})
		It("must setup an handle", func() {
			handle := MustSetupHandle(clusterURI, time.Duration(120)*time.Second)
			handle.Close()
		})
		It("should setup a secured handle", func() {
			handle, err := SetupSecuredHandle(securedURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should not be able setup a secured handle", func() {
			_, err := SetupSecuredHandle(clusterURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			Expect(err).To(HaveOccurred())
		})
		It("must setup a secured handle", func() {
			handle := MustSetupSecuredHandle(securedURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			handle.Close()
		})
		Context("With Handle", func() {
			BeforeEach(func() {
				var err error
				testHandle, err = NewHandle()
				Expect(err).ToNot(HaveOccurred())
			})
			AfterEach(func() {
				testHandle.Close()
			})
			It("should not add cluster public key with invalid filename", func() {
				err := testHandle.AddClusterPublicKey("asd")
				Expect(err).To(HaveOccurred())
			})
			It("should add cluster public key with valid file", func() {
				ioutil.WriteFile("test.key", []byte("PPm6ZeBCVlDTR9xtYasXd31s8rXnQpb+CNTMohOlQqBw="), 0777)
				err := testHandle.AddClusterPublicKey("test.key")
				Expect(err).ToNot(HaveOccurred())
				os.Remove("test.key")
			})
			It("should not add credentials with invalid filename", func() {
				err := testHandle.AddUserCredentials("asd")
				Expect(err).To(HaveOccurred())
			})
			It("should not add credentials with invalid file", func() {
				err := testHandle.AddUserCredentials("error.go")
				Expect(err).To(HaveOccurred())
			})
			It("should add credentials with valid file", func() {
				ioutil.WriteFile("test.key", []byte("{\"username\": \"vianney\",\"secret_key\": \"SeVUamemy6GWb8npfh9lum1zhdAu76W+l0PAW03G5yl4=\"}"), 0777)
				err := testHandle.AddUserCredentials("test.key")
				Expect(err).ToNot(HaveOccurred())
				os.Remove("test.key")
			})
			It("should not connect without address", func() {
				err := testHandle.Connect("")
				Expect(err).To(HaveOccurred())
			})
			It("should connect", func() {
				err := testHandle.Connect(clusterURI)
				Expect(err).ToNot(HaveOccurred())
				testHandle.Close()
			})
			Context("With Connection established", func() {
				BeforeEach(func() {
					testHandle.Connect(clusterURI)
				})
				AfterEach(func() {
					testHandle.Close()
				})
				It("should not return an empty version", func() {
					apiVersion := testHandle.APIVersion()
					Expect("").ToNot(Equal(apiVersion))
				})
				It("should not return an empty build", func() {
					apiVersion := testHandle.APIBuild()
					Expect("").ToNot(Equal(apiVersion))
				})
				It("should set timeout to 1s", func() {
					err := testHandle.SetTimeout(time.Duration(120) * time.Second)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able set timeout to 0ms", func() {
					err := testHandle.SetTimeout(time.Duration(0) * time.Millisecond)
					Expect(err).To(HaveOccurred())
				})
				It("should be able to 'set max cardinality' with default value", func() {
					err := testHandle.SetMaxCardinality(10007)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able to 'set max cardinality' with value under 100", func() {
					err := testHandle.SetMaxCardinality(99)
					Expect(err).To(HaveOccurred())
				})
				It("should be able to 'set compression' to fast", func() {
					err := testHandle.SetCompression(CompFast)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able to call 'set compression' with random value", func() {
					err := testHandle.SetCompression(5)
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
