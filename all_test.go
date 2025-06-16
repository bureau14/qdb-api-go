package qdb

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	handle       HandleType
	secureHandle HandleType
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestAll(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test Suite")
}

var _ = Describe("Tests", func() {
	BeforeSuite(func() {
		var err error
		handle, err = SetupHandle(insecureURI, 120*time.Second)
		Expect(err).ToNot(HaveOccurred())

		secureHandle, err = SetupSecuredHandle(secureURI, clusterPublicKeyFile, userPrivateKeyFile, 120*time.Second, EncryptNone)
		Expect(err).ToNot(HaveOccurred())

		SetLogFile("qdb_api.log")

		// stupid thing to boast about having 100% test coverage
		Expect(string(fmt.Errorf("error: %s", ErrorType(2)).Error())).To(Equal("error: An unknown error occurred."))
	})

	AfterSuite(func() {
		handle.Close()
		secureHandle.Close()
	})
})
