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
			err := testHandle.Connect(insecureURI)
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
		It("should setup a handle", func() {
			handle, err := SetupHandle(insecureURI, time.Duration(120)*time.Second)
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should not be able to setup a handle - secure URI with normal setup", func() {
			_, err := SetupHandle(secureURI, time.Duration(120)*time.Second)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able setup a handle - timeout set to zero", func() {
			_, err := SetupHandle(insecureURI, 0)
			Expect(err).To(HaveOccurred())
		})
		It("must setup a handle", func() {
			handle := MustSetupHandle(insecureURI, time.Duration(120)*time.Second)
			handle.Close()
		})
		It("must fail to setup a handle with invalid parameters", func() {
			Expect(func() {
				MustSetupHandle("", 0)
			}).Should(Panic())
		})
		It("should setup a secure handle", func() {
			handle, err := SetupSecuredHandle(secureURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			Expect(err).ToNot(HaveOccurred())
			handle.Close()
		})
		It("should not be able setup a secure handle - insecure cluster uri", func() {
			_, err := SetupSecuredHandle(insecureURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able setup a secure handle - missing cluster key", func() {
			_, err := SetupSecuredHandle(secureURI, "", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able setup a secure handle - missing user key", func() {
			_, err := SetupSecuredHandle(secureURI, "cluster_public.key", "", time.Duration(120)*time.Second, EncryptNone)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able setup a secure handle - timeout set to zero", func() {
			_, err := SetupSecuredHandle(secureURI, "cluster_public.key", "user_private.key", 0, EncryptNone)
			Expect(err).To(HaveOccurred())
		})
		It("should not be able setup a secure handle - encrypt has random value", func() {
			_, err := SetupSecuredHandle(secureURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, 123)
			Expect(err).To(HaveOccurred())
		})
		It("must setup a secure handle", func() {
			handle := MustSetupSecuredHandle(secureURI, "cluster_public.key", "user_private.key", time.Duration(120)*time.Second, EncryptNone)
			handle.Close()
		})
		It("must fail to setup a secure handle with invalid parameters", func() {
			Expect(func() {
				MustSetupSecuredHandle(insecureURI, "", "", 0, 123)
			}).Should(Panic())
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
			It("should get last error", func() {
				err := testHandle.Connect("")
				message, last_err := testHandle.GetLastError()
				Expect(err).To(Equal(last_err))
				Expect(string("at qdb_connect: Got NULL uri")).To(Equal(message))
			})
			It("should not add cluster public key with invalid filename", func() {
				_, err := ClusterKeyFromFile("asd")
				Expect(err).To(HaveOccurred())
			})
			It("should add cluster public key with valid file", func() {
				ioutil.WriteFile("test.key", []byte("PPm6ZeBCVlDTR9xtYasXd31s8rXnQpb+CNTMohOlQqBw="), 0777)
				_, err := ClusterKeyFromFile("test.key")
				Expect(err).ToNot(HaveOccurred())
				os.Remove("test.key")
			})
			It("should not add credentials with invalid filename", func() {
				_, _, err := UserCredentialFromFile("asd")
				Expect(err).To(HaveOccurred())
			})
			It("should not add credentials with invalid file", func() {
				_, _, err := UserCredentialFromFile("error.go")
				Expect(err).To(HaveOccurred())
			})
			It("should add credentials with valid file", func() {
				ioutil.WriteFile("test.key", []byte("{\"username\": \"vianney\",\"secret_key\": \"SeVUamemy6GWb8npfh9lum1zhdAu76W+l0PAW03G5yl4=\"}"), 0777)
				user, secret, err := UserCredentialFromFile("test.key")
				Expect(err).ToNot(HaveOccurred())
				Expect(string("vianney")).To(Equal(user))
				Expect(string("SeVUamemy6GWb8npfh9lum1zhdAu76W+l0PAW03G5yl4=")).To(Equal(secret))
				os.Remove("test.key")
			})
			It("should not connect without address", func() {
				err := testHandle.Connect("")
				Expect(err).To(HaveOccurred())
			})
			It("should connect", func() {
				err := testHandle.Connect(insecureURI)
				Expect(err).ToNot(HaveOccurred())
				testHandle.Close()
			})
			Context("With Connection established", func() {
				BeforeEach(func() {
					testHandle.Connect(insecureURI)
				})
				AfterEach(func() {
					testHandle.Close()
				})
				It("should return empty string message and nil error", func() {
					message, last_err := testHandle.GetLastError()
					Expect(message).To(BeEmpty())
					Expect(last_err).ToNot(HaveOccurred())
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
				It("should be able to call 'set client max in buf size'", func() {
					err := testHandle.SetClientMaxInBufSize(100000000)
					Expect(err).ToNot(HaveOccurred())
				})
				It("should not be able to call 'set client max in buf size' with a value too small", func() {
					err := testHandle.SetClientMaxInBufSize(100)
					Expect(err).To(HaveOccurred())
				})
				It("should be able to get client max in buf size", func() {
					err := testHandle.SetClientMaxInBufSize(100000000)
					Expect(err).ToNot(HaveOccurred())
					result, err := testHandle.GetClientMaxInBufSize()
					Expect(err).ToNot(HaveOccurred())
					Expect(uint(result)).To(Equal(uint(100000000)))
				})
				It("should be able to get cluster max in buf size", func() {
					result, err := testHandle.GetClusterMaxInBufSize()
					Expect(err).ToNot(HaveOccurred())
					Expect(result).ToNot(Equal(0))
				})
				It("should be able to get client max parallelism", func() {
					err := testHandle.SetClientMaxParallelism(8)
					Expect(err).ToNot(HaveOccurred())
					result, err := testHandle.GetClientMaxParallelism()
					Expect(err).ToNot(HaveOccurred())
					Expect(uint(result)).To(Equal(uint(8)))
				})
				Context("with an entry added", func() {
					var (
						alias   = generateAlias(16)
						integer IntegerEntry
					)
					BeforeEach(func() {
						integer = testHandle.Integer(alias)
						integer.Put(8, NeverExpires())
					})
					AfterEach(func() {
						integer.Remove()
					})
					It("'get tags' should work with no tags added", func() {
						tags, err := testHandle.GetTags(alias)
						Expect(err).ToNot(HaveOccurred())
						Expect([]string{}).To(Equal(tags))
					})
					It("'get tags' should not work with a bad alias", func() {
						tags, err := testHandle.GetTags("")
						Expect(err).To(HaveOccurred())
						Expect([]string(nil)).To(Equal(tags))
					})
					It("'get tags' should work with alias", func() {
						err := integer.AttachTag("tag")
						Expect(err).ToNot(HaveOccurred())
						tags, err := testHandle.GetTags(alias)
						Expect(err).ToNot(HaveOccurred())
						Expect([]string{"tag"}).To(Equal(tags))
					})
					It("'get tagged' should work with \"tag\"", func() {
						integer.AttachTag("tag")
						entries, err := testHandle.GetTagged("tag")
						Expect(err).ToNot(HaveOccurred())
						Expect([]string{alias}).To(Equal(entries))
					})
					It("'get tagged' should return empty results with bad tag", func() {
						tags, err := testHandle.GetTagged("bad")
						Expect(err).ToNot(HaveOccurred())
						Expect([]string{}).To(Equal(tags))
					})
					It("'prefix get' should return empty results with bad prefix", func() {
						entries, err := testHandle.PrefixGet("bad", 10)
						Expect(err).To(Equal(ErrAliasNotFound))
						Expect([]string{}).To(Equal(entries))
					})
					It("'prefix get' should return alias value with proper prefix", func() {
						entries, err := testHandle.PrefixGet(alias[:3], 10)
						Expect(err).ToNot(HaveOccurred())
						Expect([]string{alias}).To(Equal(entries))
					})
					It("'prefix count' should count no results with bad prefix", func() {
						count, err := testHandle.PrefixCount("bad")
						Expect(err).ToNot(HaveOccurred())
						Expect(uint64(0)).To(Equal(count))
					})
					It("'prefix count' should count one entry with proper prefix", func() {
						count, err := testHandle.PrefixCount(alias[:3])
						Expect(err).ToNot(HaveOccurred())
						Expect(uint64(1)).To(Equal(count))
					})
				})
			})
		})
	})
})
